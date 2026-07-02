"""
raft_node.py — Nó Raft com gRPC

Cada processo executa uma instância desta classe, que:
  - Atua como SERVIDOR gRPC (herda de RaftServiceServicer)
  - Gerencia o ciclo de eleição (follower → candidate → leader)
  - Delega replicação de log à classe Replication (replication.py)
  - Persiste estado crítico em disco (current_term, voted_for, log)
"""
import sys
import grpc
import raft_pb2
import raft_pb2_grpc
import config
import threading
import time
import random
from concurrent import futures

from raft_log import RaftLog
from replication import Replication
from persistence import Persistence


class RaftNode(Replication, raft_pb2_grpc.RaftServiceServicer):
    """
    Nó Raft completo. Herda de:
      - Replication: lógica de AppendEntries e replicação de log
      - RaftServiceServicer: interface gRPC gerada pelo proto (servidor)
    """

    def __init__(self, node_id: int):
        self.node_id = node_id
        self.addr = config.NODES[node_id]   # endereço gRPC deste nó (host:porta)
        self.state_machine = []

        # --- Persistência ---
        # Criada primeiro pois o carregamento do estado depende dela
        self._persistence = Persistence(node_id)

        # --- Estado persistente Raft ---
        self.current_term = 0     # último termo que este nó conhece
        self.voted_for = None     # ID do candidato em que votou no termo atual (ou None)

        # --- Log replicado ---
        self.log = RaftLog()

        # Tenta carregar estado salvo em disco (recuperação após crash)
        self._load_persisted_state()

        # --- Estado volátil (sempre reinicia do zero, independente do crash) ---
        self.role = 'follower'    # papel atual: SEMPRE começa como follower
        self.leader_id = None     # ID do líder conhecido (aprendido via heartbeats)

        self.commit_index = 0    # índice da entrada mais recente que sabemos estar commitada
        self.last_applied = 0    # índice da entrada mais recente aplicada à state machine

        # --- Estado do líder (só válido quando role == 'leader') ---
        self.next_index = {}     # next_index[peer] = próximo índice a enviar ao peer
        self.match_index = {}    # match_index[peer] = índice mais alto replicado no peer

        # --- Eleição ---
        self.votes_received = 0

        # Lock para proteger transições de estado (evita race condition
        # onde dois threads contam votos simultaneamente e elegem o líder duas vezes)
        self._state_lock = threading.Lock()

        # Condition variable usada pelo ReceiveCommand para aguardar o commit
        # _apply_commits notifica esta condition sempre que um commit acontece
        self._commit_condition = threading.Condition()

        # Inicia temporizador de eleição
        self.next_timeout_at = 0
        self._reset_election_timer()

        # Thread que monitora o timeout de eleição em background
        threading.Thread(target=self._watch_election_timeout, daemon=True).start()

    # =========================================================================
    # PERSISTÊNCIA
    # =========================================================================

    def _save_state(self):
        """
        Persiste current_term, voted_for e o log em disco.

        Deve ser chamado SEMPRE que current_term, voted_for ou o log mudarem,
        ANTES de enviar a resposta RPC correspondente. Isso garante que, se o
        processo morrer no meio de uma operação, o estado recuperado do disco
        é consistente com o que o nó já prometeu aos outros.
        """
        self._persistence.save(
            current_term=self.current_term,
            voted_for=self.voted_for,
            log_entries=self.log.entries,
        )

    def _load_persisted_state(self):
        """
        Carrega estado do disco ao iniciar.
        Se não há arquivo (primeira execução), mantém os valores padrão.
        """
        state = self._persistence.load()
        if state is None:
            print(f"[NÓ {self.node_id}] Primeira inicialização — sem estado persistido.")
            return

        self.current_term = state["current_term"]
        self.voted_for    = state["voted_for"]    # pode ser None (null no JSON)
        self.log.entries  = self._persistence.restore_log(state["log"])

        print(
            f"[NÓ {self.node_id}] Estado restaurado do disco: "
            f"term={self.current_term}, voted_for={self.voted_for}, "
            f"log_size={self.log.get_last_index()}"
        )

    # =========================================================================
    # TEMPORIZADOR DE ELEIÇÃO
    # =========================================================================

    def _reset_election_timer(self):
        """Reinicia o timeout de eleição com valor aleatório dentro do intervalo configurado."""
        timeout = random.uniform(config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX)
        self.next_timeout_at = time.time() + timeout

    def _watch_election_timeout(self):
        """
        Loop em background que dispara eleição quando o timeout expira.
        Roda a cada 10ms para ter boa precisão sem consumir CPU excessivamente.
        """
        while True:
            time.sleep(0.01)
            if self.role != 'leader' and time.time() > self.next_timeout_at:
                self._start_election()

    # =========================================================================
    # ELEIÇÃO
    # =========================================================================

    def _start_election(self):
        """
        Transição follower → candidate.
        Incrementa o termo, vota em si mesmo e pede votos aos peers em paralelo.
        """
        with self._state_lock:
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = 1       # voto em si mesmo
            self.leader_id = None
            self._reset_election_timer()  # reinicia para o caso de split vote
            self._save_state()            # persiste novo termo e voto em si mesmo

        print(f"[NÓ {self.node_id}] Iniciando eleição → TERMO {self.current_term}")

        # Pede voto a cada peer em uma thread separada (para não bloquear o timer)
        for peer_id, addr in config.NODES.items():
            if peer_id != self.node_id:
                threading.Thread(
                    target=self._ask_for_vote,
                    args=(peer_id, addr),
                    daemon=True
                ).start()

    def _ask_for_vote(self, peer_id: int, addr: str):
        """
        Envia RequestVote RPC a um peer via gRPC.

        DIFERENÇA DO PYRO5:
        - Antes: Pyro5.api.Proxy(uri) + proxy.request_vote(term, candidateId, ...)
        - Agora: grpc.insecure_channel(addr) + stub.RequestVote(RequestVoteRequest(...))
        O retorno é um objeto protobuf (reply.term, reply.vote_granted) em vez de uma tupla.
        """
        try:
            # Captura os valores atuais para evitar race condition ao ler durante o RPC
            term_snapshot = self.current_term

            with grpc.insecure_channel(addr) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                request = raft_pb2.RequestVoteRequest(
                    term=term_snapshot,
                    candidate_id=self.node_id,
                    last_log_index=self.log.get_last_index(),
                    last_log_term=self.log.get_last_term(),
                )
                reply = stub.RequestVote(request, timeout=0.5)

            # Se o peer tem termo maior, voltamos a ser follower imediatamente
            if reply.term > self.current_term:
                with self._state_lock:
                    self.current_term = reply.term
                    self.role = 'follower'
                    self.voted_for = None
                    self._reset_election_timer()

            # Se ganhou o voto e ainda somos candidato no mesmo termo
            elif reply.vote_granted and self.role == 'candidate' and term_snapshot == self.current_term:
                self._count_votes()

        except Exception:
            pass  # Peer offline ou timeout — ignorar, será tentado novamente se houver nova eleição

    def _count_votes(self):
        """
        Contabiliza um voto recebido. Se atingiu maioria, torna-se líder.
        Protegido por lock pois múltiplos threads chamam simultaneamente.
        """
        with self._state_lock:
            self.votes_received += 1
            majority = len(config.NODES) / 2
            if self.votes_received > majority and self.role != 'leader':
                self._become_leader()

    def _become_leader(self):
        """
        Transição candidate → leader.
        Inicializa next_index e match_index para todos os peers e
        começa a enviar heartbeats periódicos.
        """
        self.role = 'leader'
        self.leader_id = self.node_id

        # next_index começa no índice logo após o último do nosso log
        self.next_index = {
            p: self.log.get_last_index() + 1
            for p in config.NODES if p != self.node_id
        }
        # match_index começa em 0 (não confirmamos nada ainda)
        self.match_index = {p: 0 for p in config.NODES if p != self.node_id}

        print(f"\n{'='*50}")
        print(f"[NÓ {self.node_id}] *** ELEITO LÍDER NO TERMO {self.current_term} ***")
        print(f"{'='*50}\n")

        # Inicia thread de heartbeats — fica enviando AppendEntries periodicamente
        threading.Thread(target=self._send_heartbeats, daemon=True).start()

    def _send_heartbeats(self):
        """
        Loop do líder: envia AppendEntries (heartbeat ou com entradas) a todos os
        followers a cada HEARTBEAT_INTERVAL segundos.
        Para quando este nó deixa de ser líder.
        """
        while self.role == 'leader':
            self._replicate_to_followers()
            time.sleep(config.HEARTBEAT_INTERVAL)

    # =========================================================================
    # HANDLER gRPC: RequestVote
    # =========================================================================

    def RequestVote(self, request, context):
        """
        Handler gRPC chamado quando um candidato pede nosso voto.

        DIFERENÇA DO PYRO5:
        - Antes: def request_vote(self, term, candidate_id, ...) retornava (term, bool)
        - Agora: def RequestVote(self, request, context) retorna RequestVoteReply(...)
          Os parâmetros vêm em 'request.term', 'request.candidate_id', etc.
          O retorno é um objeto protobuf, não uma tupla Python.
        """
        term = request.term
        candidate_id = request.candidate_id
        last_log_index = request.last_log_index
        last_log_term = request.last_log_term

        # Regra 1: Rejeita se o candidato tem termo mais antigo que o nosso
        if term < self.current_term:
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=False)

        # Verifica se o log do candidato é pelo menos tão atualizado quanto o nosso.
        # "Mais atualizado" = (termo mais alto) ou (mesmo termo com índice >= nosso índice)
        my_last_term = self.log.get_last_term()
        my_last_index = self.log.get_last_index()
        log_ok = (last_log_term > my_last_term) or \
                 (last_log_term == my_last_term and last_log_index >= my_last_index)

        # Regra 2: Se o candidato tem termo maior, atualizamos e viramos follower
        if term > self.current_term:
            self.current_term = term
            self.role = 'follower'
            self.voted_for = None

        # Regra 3: Concede voto se (ainda não votamos OU já votamos neste candidato)
        #          E o log do candidato está atualizado
        if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            self.voted_for = candidate_id
            self._reset_election_timer()  # recebemos comunicação válida, reinicia timer
            self._save_state()            # persiste o voto antes de responder
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=True)

        return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=False)

    # =========================================================================
    # STATE MACHINE
    # =========================================================================

    def _apply_to_state_machine(self, entry: str):
        """Aplica uma entrada commitada à state machine (aqui apenas imprime)."""
        self.state_machine.append(entry)
        print(entry)

    def get_state(self) -> dict:
        """Retorna o estado atual do nó (usado para debug/monitoramento)."""
        return {
            'node_id': self.node_id,
            'term': self.current_term,
            'role': self.role,
            'leader_id': self.leader_id,
            'log_size': self.log.get_last_index(),
            'commit_index': self.commit_index,
        }


# =============================================================================
# PONTO DE ENTRADA
# =============================================================================

def main():
    """
    Inicia o servidor gRPC do nó Raft.

    DIFERENÇA DO PYRO5:
    - Antes: Daemon(host, port) + daemon.register(obj) + daemon.requestLoop()
    - Agora: grpc.server(...) + add_RaftServiceServicer_to_server(obj, server)
             + server.add_insecure_port(addr) + server.start() + server.wait_for_termination()

    O grpc.server recebe um ThreadPoolExecutor que define quantas chamadas RPC
    simultâneas o servidor consegue processar.
    """
    if len(sys.argv) < 2:
        print("Uso: python raft_node.py <node_id>")
        sys.exit(1)

    node_id = int(sys.argv[1])
    addr = config.NODES[node_id]

    raft_node = RaftNode(node_id)

    # Cria o servidor gRPC com pool de threads para tratar RPCs concorrentes
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Registra o nó como implementação do serviço RaftService
    raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_node, server)

    # Bind na porta configurada (sem TLS — insecure)
    server.add_insecure_port(addr)
    server.start()

    print(f"[NÓ {node_id}] Servidor gRPC iniciado em {addr}")
    server.wait_for_termination()


if __name__ == '__main__':
    main()