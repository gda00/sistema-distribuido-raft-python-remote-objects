"""
client.py — Cliente Raft com gRPC

Envia comandos ao cluster Raft e lê dados committed.
Tenta todos os nós em ordem até encontrar o líder.
Se um nó responder com leader_hint, reconecta diretamente ao líder.

DIFERENÇA DO PYRO5:
- Antes: conectava ao NameServer para descobrir o URI do líder (PYRO:Leader@...)
- Agora: tenta cada nó em ordem até receber success=True
         Se receber leader_hint, vai direto ao líder indicado

OPERAÇÕES DISPONÍVEIS:
- publish <dado> : envia dado ao cluster (write, via ReceiveCommand)
- consume        : lê dados committed de qualquer nó (via ReadData)
- consume leader : força leitura no líder (garantia de linearizabilidade)
"""
import grpc
import time
import config
import raft_pb2
import raft_pb2_grpc


class RaftCliente:

    def __init__(self):
        self.leader_addr: str | None = None  # endereço gRPC do líder atual

    # =========================================================================
    # ESCRITA (PUBLISH)
    # =========================================================================

    def _try_node(self, addr: str, command: str) -> tuple[bool, str]:
        """
        Tenta enviar um comando a um nó específico.

        Retorna:
            (True, '')           — comando aceito e commitado
            (False, leader_hint) — nó não é o líder; leader_hint pode ser o endereço do líder
            (False, '')          — nó offline ou erro
        """
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                req = raft_pb2.CommandRequest(command=command)
                reply = stub.ReceiveCommand(req, timeout=6.0)  # 6s > 5s de timeout do commit

                if reply.success:
                    return True, ''
                else:
                    return False, reply.leader_hint
        except grpc.RpcError:
            return False, ''  # nó offline ou timeout
        except Exception:
            return False, ''

    def send_command(self, command: str) -> bool:
        """
        Envia um comando ao cluster com auto-descoberta do líder.

        Algoritmo:
          1. Se já temos o endereço do líder, tentamos direto nele.
          2. Se falhar, varremos todos os nós para encontrar o novo líder.
          3. Se algum nó der um leader_hint, usamos esse hint diretamente.
        """
        # 1. Tenta no líder conhecido primeiro (evita varrer todos os nós)
        if self.leader_addr:
            success, hint = self._try_node(self.leader_addr, command)
            if success:
                return True
            if hint and hint != self.leader_addr:
                # O nó nos redirecionou para outro líder
                self.leader_addr = hint
                success, _ = self._try_node(self.leader_addr, command)
                if success:
                    print(f"[CLIENTE] Reconectado ao líder em {self.leader_addr}")
                    return True
            # Líder anterior não está mais disponível
            self.leader_addr = None

        # 2. Varre todos os nós em ordem
        all_addrs = list(config.NODES.values())
        for addr in all_addrs:
            success, hint = self._try_node(addr, command)
            if success:
                self.leader_addr = addr
                print(f"[CLIENTE] Líder encontrado em {addr}")
                return True
            if hint and hint in all_addrs:
                # Dica: vai direto ao líder sugerido
                success2, _ = self._try_node(hint, command)
                if success2:
                    self.leader_addr = hint
                    print(f"[CLIENTE] Líder encontrado (via hint) em {hint}")
                    return True

        print("[CLIENTE] Líder não encontrado. Cluster indisponível?")
        return False

    # =========================================================================
    # LEITURA (CONSUME)
    # =========================================================================

    def read_data(self, addr: str) -> raft_pb2.ReadReply | None:
        """
        Lê dados committed de um nó específico via ReadData RPC.

        Retorna o ReadReply ou None em caso de erro.
        A leitura é permitida em QUALQUER nó (líder ou réplica).
        Apenas dados com índice <= commit_index são retornados (nunca uncommitted).
        """
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                req = raft_pb2.ReadRequest()
                return stub.ReadData(req, timeout=5.0)
        except grpc.RpcError:
            return None
        except Exception:
            return None

    def consume(self, force_leader: bool = False) -> bool:
        """
        Lê e exibe todos os dados committed do cluster.

        Parâmetros:
            force_leader : se True, garante que a leitura é feita no líder
                           (leitura forte / linearizável).
                           Se False, lê de qualquer nó disponível
                           (consistência eventual — pode estar ligeiramente atrás).

        Retorna True se a leitura foi bem-sucedida.
        """
        if force_leader:
            return self._consume_from_leader()
        else:
            return self._consume_from_any()

    def _consume_from_any(self) -> bool:
        """Lê de qualquer nó disponível (consistência eventual)."""
        all_addrs = list(config.NODES.values())
        for addr in all_addrs:
            reply = self.read_data(addr)
            if reply and reply.success:
                self._print_entries(addr, reply)
                # Atualiza o líder conhecido se o nó informar
                if reply.leader_hint:
                    self.leader_addr = reply.leader_hint
                return True

        print("[CLIENTE] ✗ Nenhum nó disponível para leitura.")
        return False

    def _consume_from_leader(self) -> bool:
        """Lê apenas do líder (leitura forte / linearizável)."""
        # Tenta o líder conhecido primeiro
        if self.leader_addr:
            reply = self.read_data(self.leader_addr)
            if reply and reply.success:
                self._print_entries(self.leader_addr, reply)
                return True
            self.leader_addr = None  # líder anterior inválido

        # Descobre o líder via hint de qualquer nó
        all_addrs = list(config.NODES.values())
        for addr in all_addrs:
            reply = self.read_data(addr)
            if reply and reply.success and reply.leader_hint:
                leader = reply.leader_hint
                self.leader_addr = leader
                # Agora lê diretamente do líder
                leader_reply = self.read_data(leader)
                if leader_reply and leader_reply.success:
                    self._print_entries(leader, leader_reply)
                    return True

        print("[CLIENTE] ✗ Não foi possível determinar o líder para leitura forte.")
        return False

    def _print_entries(self, addr: str, reply: raft_pb2.ReadReply):
        """Exibe as entradas committed retornadas pelo nó."""
        print(f"\n[CLIENTE] Leitura de {addr} | commit_index={reply.commit_index}")
        print("─" * 45)
        if not reply.entries:
            print("  (nenhum dado committed ainda)")
        else:
            for i, entry in enumerate(reply.entries, start=1):
                print(f"  [{i}] {entry}")
        print("─" * 45)

    # =========================================================================
    # LOOP INTERATIVO
    # =========================================================================

    def run(self):
        print("=" * 50)
        print("    Cliente Raft (gRPC) — Python")
        print("=" * 50)
        print("Comandos disponíveis:")
        print("  publish <dado>   — envia dado ao cluster (write)")
        print("  consume          — lê dados de qualquer nó (eventual)")
        print("  consume leader   — lê dados do líder (forte)")
        print("  sair / exit      — encerra o cliente")
        print()

        while True:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n[CLIENTE] Encerrando.")
                break

            if not line:
                continue

            parts = line.split()
            cmd = parts[0].lower()

            if cmd in ('sair', 'exit', 'quit'):
                break

            elif cmd == 'publish':
                if len(parts) < 2:
                    print("[CLIENTE] Uso: publish <dado>")
                    continue

                data = ' '.join(parts[1:])
                sucesso = self.send_command(data)

                # Retentar automaticamente se o cluster está em eleição
                retries = 0
                while not sucesso and retries < 5:
                    retries += 1
                    print(f"[CLIENTE] Aguardando líder... (tentativa {retries}/5)")
                    time.sleep(2)
                    sucesso = self.send_command(data)

                if sucesso:
                    print(f"[CLIENTE] ✓ '{data}' publicado com sucesso!\n")
                else:
                    print(f"[CLIENTE] ✗ Falha ao publicar '{data}' após {retries} tentativas.\n")

            elif cmd == 'consume':
                force_leader = len(parts) > 1 and parts[1].lower() == 'leader'
                self.consume(force_leader=force_leader)

            else:
                # Compatibilidade retroativa: qualquer outro texto é tratado como publish
                sucesso = self.send_command(line)

                retries = 0
                while not sucesso and retries < 5:
                    retries += 1
                    print(f"[CLIENTE] Aguardando líder... (tentativa {retries}/5)")
                    time.sleep(2)
                    sucesso = self.send_command(line)

                if sucesso:
                    print(f"[CLIENTE] ✓ Comando '{line}' commitado com sucesso!\n")
                else:
                    print(f"[CLIENTE] ✗ Falha ao enviar '{line}' após {retries} tentativas.\n")


if __name__ == "__main__":
    cliente = RaftCliente()
    cliente.run()