"""
replication.py — Lógica de replicação de log do Raft (AppendEntries)

Esta classe é um mixin herdado por RaftNode.
Contém os handlers gRPC de AppendEntries e ReceiveCommand,
e a lógica interna de replicação para os peers (envio, commit, aplicação).
"""
import grpc
import raft_pb2
import raft_pb2_grpc
from raft_log import LogEntry
import config
import threading
import time


class Replication:

    # =========================================================================
    # HANDLER gRPC: AppendEntries
    # =========================================================================

    def AppendEntries(self, request, context):
        """
        Handler gRPC chamado pelo líder para replicar entradas de log OU enviar heartbeat.

        DIFERENÇA DO PYRO5:
        - Antes: def append_entries(self, term, leader_id, ..., entries, leader_commit)
                 'entries' era uma lista de dicts Python
                 Retornava tupla: (self.current_term, True/False)
        - Agora: def AppendEntries(self, request, context)
                 Parâmetros vêm em request.term, request.entries, etc.
                 'request.entries' é uma lista de objetos raft_pb2.LogEntry (protobuf)
                 Retorna objeto: raft_pb2.AppendEntriesReply(term=..., success=...)

        Heartbeat = AppendEntries com entries vazio (len == 0).
        O efeito de reiniciar o election timer acontece mesmo no heartbeat.
        """
        term = request.term
        leader_id = request.leader_id
        prev_log_index = request.prev_log_index
        prev_log_term = request.prev_log_term
        entries = request.entries        # lista de raft_pb2.LogEntry
        leader_commit = request.leader_commit

        # Regra 1: Rejeita mensagem de líder com termo desatualizado
        if term < self.current_term:
            return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)

        # Regra 2: Mensagem válida de um líder — atualiza estado e reinicia timer
        term_changed = term > self.current_term
        if term >= self.current_term:
            self.current_term = term
            self.role = 'follower'
            self.voted_for = None
            self.leader_id = leader_id   # registra o líder atual (para redirecionar clientes)
            self._reset_election_timer()

        # Persiste imediatamente se o termo mudou (§5.1 do paper do Raft):
        # current_term deve ser durável antes de qualquer resposta RPC
        if term_changed:
            self._save_state()

        # Regra 3: Verifica consistência do log — o prev_log deve bater com o nosso
        # Se não bater, retorna False para o líder retroceder o nextIndex
        if not self.log.match_entry(prev_log_index, prev_log_term):
            return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)

        # Regra 4: Processa as entradas novas (vazio no caso de heartbeat)
        log_changed = False
        for entry_data in entries:
            # entry_data é um raft_pb2.LogEntry (objeto protobuf)
            # — diferente do Pyro5 onde chegava como dict Python
            idx = entry_data.index
            trm = entry_data.term
            cmd = entry_data.command

            existing = self.log.get_entry(idx)

            # Conflito: mesma posição mas termo diferente → apaga tudo a partir daí
            if existing and existing.term != trm:
                self.log.delete_from(idx)
                existing = None
                log_changed = True

            # Adiciona somente se ainda não existe
            if not existing:
                self.log.append(LogEntry(idx, trm, cmd))
                log_changed = True

        # Persiste se o log mudou (term já foi persistido junto com a mudança acima)
        if log_changed:
            self._save_state()

        # Regra 5: Avança o commitIndex se o líder informou um commit maior
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.get_last_index())
            self._apply_commits()

        return raft_pb2.AppendEntriesReply(term=self.current_term, success=True)

    # =========================================================================
    # HANDLER gRPC: ReadData (leitura de dados committed)
    # =========================================================================

    def ReadData(self, request, context):
        """
        Handler gRPC chamado pelo cliente para ler dados committed do cluster.

        Comportamento:
          - Pode ser chamado em QUALQUER nó (líder ou réplica).
          - Retorna APENAS entradas com índice <= commit_index (dados committed).
          - Nunca expõe entradas uncommitted (acima do commit_index).
          - Inclui leader_hint para que o cliente possa ir ao líder se quiser
            garantia de leitura forte (linearizável).

        Leitura em réplica oferece consistência eventual — o commit_index local
        pode estar ligeiramente atrás do líder, mas NUNCA retorna dados não
        confirmados pela maioria.
        """
        # Coleta todas as entradas committed (índice 1 até commit_index)
        # Índice 0 é a entrada sentinela (command=None), por isso começa em 1
        committed_entries = [
            entry.command
            for entry in self.log.entries[1:self.commit_index + 1]
            if entry.command is not None
        ]

        # Informa o endereço do líder (se conhecido) para leitura forte
        leader_addr = ''
        if self.leader_id is not None:
            leader_addr = config.NODES.get(self.leader_id, '')

        return raft_pb2.ReadReply(
            success=True,
            entries=committed_entries,
            commit_index=self.commit_index,
            leader_hint=leader_addr,
        )

    # =========================================================================
    # HANDLER gRPC: ReceiveCommand (interface com o cliente)
    # =========================================================================

    def ReceiveCommand(self, request, context):
        """
        Handler gRPC chamado pelo cliente para enviar um comando ao cluster.

        Comportamento:
          1. Se não somos líder, devolve leader_hint para o cliente se reconectar.
          2. Se somos líder, anexa a entrada ao log, persiste, e replica para os peers.
          3. BLOQUEIA até que a entrada seja commitada (commit_index ≥ new_index)
             ou até 5 segundos de timeout (caso percamos a liderança).

        O bloqueio usa threading.Condition (_commit_condition), que é sinalizado
        pelo _apply_commits sempre que o commit_index avança.
        """
        if self.role != 'leader':
            # Informa ao cliente quem é o líder (se soubermos)
            leader_addr = config.NODES.get(self.leader_id, '') if self.leader_id is not None else ''
            return raft_pb2.CommandReply(success=False, leader_hint=leader_addr)

        command = request.command
        new_index = self.log.get_last_index() + 1
        entry = LogEntry(new_index, self.current_term, command)
        self.log.append(entry)
        self._save_state()  # persiste a nova entrada ANTES de replicar
        print(f"[LÍDER {self.node_id}] Comando recebido: '{command}' (índice {new_index}) → replicando...")

        # Dispara replicação em paralelo para todos os peers
        self._replicate_to_followers()

        # Aguarda o commit desta entrada específica
        # Timeout de 5s como proteção caso percamos a liderança durante a replicação
        deadline = time.time() + 5.0
        with self._commit_condition:
            while self.commit_index < new_index:
                remaining = deadline - time.time()
                if remaining <= 0:
                    # Timeout — provavelmente perdemos o quorum
                    return raft_pb2.CommandReply(success=False, leader_hint='')
                if self.role != 'leader':
                    # Perdemos a liderança enquanto aguardavamos
                    return raft_pb2.CommandReply(success=False, leader_hint='')
                self._commit_condition.wait(timeout=min(remaining, 0.1))

        return raft_pb2.CommandReply(success=True)

    # =========================================================================
    # REPLICAÇÃO INTERNA (líder → peers)
    # =========================================================================

    def _replicate_to_followers(self):
        """Dispara uma thread de replicação para cada peer em paralelo."""
        for peer_id, addr in config.NODES.items():
            if peer_id != self.node_id:
                threading.Thread(
                    target=self._send_to_peer,
                    args=(peer_id, addr),
                    daemon=True
                ).start()

    def _send_to_peer(self, peer_id: int, addr: str):
        """
        Envia AppendEntries a um peer específico via gRPC.

        DIFERENÇA DO PYRO5:
        - Antes: Pyro5.api.Proxy(uri) + proxy.append_entries(term, ..., entries_dicts, ...)
                 'entries_dicts' era lista de dicts {'index': ..., 'term': ..., 'command': ...}
        - Agora: grpc.insecure_channel(addr) + stub.AppendEntries(AppendEntriesRequest(...))
                 'entries' é lista de raft_pb2.LogEntry (objetos protobuf)
                 O retorno é reply.term e reply.success em vez de (term, success)
        """
        if self.role != 'leader':
            return

        next_idx = self.next_index[peer_id]
        prev_log_index = next_idx - 1

        prev_entry = self.log.get_entry(prev_log_index)
        prev_log_term = prev_entry.term if prev_entry else 0

        # Entradas a enviar a partir de next_idx (pode ser lista vazia no heartbeat)
        local_entries = self.log.get_entries_from(next_idx)

        # Converte LogEntry local → raft_pb2.LogEntry (objeto protobuf)
        pb_entries = [
            raft_pb2.LogEntry(index=e.index, term=e.term, command=e.command)
            for e in local_entries
        ]

        try:
            with grpc.insecure_channel(addr) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                req = raft_pb2.AppendEntriesRequest(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=prev_log_index,
                    prev_log_term=prev_log_term,
                    entries=pb_entries,
                    leader_commit=self.commit_index,
                )
                reply = stub.AppendEntries(req, timeout=2.0)

            # Peer tem termo maior → voltamos a ser follower
            if reply.term > self.current_term:
                self.current_term = reply.term
                self.role = 'follower'
                self.voted_for = None
                self._reset_election_timer()
                return

            if reply.success:
                if local_entries:
                    # Avança os ponteiros deste peer
                    self.next_index[peer_id] = local_entries[-1].index + 1
                    self.match_index[peer_id] = local_entries[-1].index
                    self._check_commit()
            else:
                # Log inconsistente — retrocede next_index e tentará novamente
                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)

        except Exception:
            pass  # Peer offline ou timeout — será tentado no próximo heartbeat

    # =========================================================================
    # COMMIT E APLICAÇÃO
    # =========================================================================

    def _check_commit(self):
        """
        Verifica se alguma entrada ainda não commitada já foi replicada na maioria dos nós.
        Se sim, avança o commitIndex e aplica os commits pendentes.
        """
        for N in range(self.commit_index + 1, self.log.get_last_index() + 1):
            # Conta quantos nós já replicaram a entrada N (líder conta como 1)
            replicated = 1 + sum(
                1 for idx in self.match_index.values() if idx >= N
            )
            majority = len(config.NODES) / 2

            # Só commita entradas do termo ATUAL (regra de segurança do Raft)
            if replicated > majority and self.log.get_entry(N).term == self.current_term:
                self.commit_index = N
                self._apply_commits()

    def _apply_commits(self):
        """
        Aplica à state machine todas as entradas commitadas ainda não aplicadas.

        Após aplicar, persiste o estado em disco (incluindo o log com as entradas
        committed). Isso garante que, após um crash e reinício, o nó saiba quais
        entradas já foram aplicadas e não as aplique em duplicata.
        """
        applied_any = False
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get_entry(self.last_applied)
            if entry and entry.command:
                self._apply_to_state_machine(
                    f"[COMMIT] índice {entry.index} | termo {entry.term} | comando: '{entry.command}'"
                )
                applied_any = True

        # Persiste o estado após aplicar commits (garante durabilidade pós-crash)
        # Só salva se houve mudança, para evitar writes desnecessários
        if applied_any:
            self._save_state()

        # Notifica todos os threads bloqueados no ReceiveCommand aguardando este commit
        with self._commit_condition:
            self._commit_condition.notify_all()
