"""
client.py — Cliente Raft com gRPC

Envia comandos ao cluster Raft. Tenta todos os nós em ordem até encontrar
o líder. Se um nó responder com leader_hint, reconecta diretamente ao líder.

DIFERENÇA DO PYRO5:
- Antes: conectava ao NameServer para descobrir o URI do líder (PYRO:Leader@...)
- Agora: tenta cada nó em ordem até receber success=True
         Se receber leader_hint, vai direto ao líder indicado
"""
import grpc
import time
import config
import raft_pb2
import raft_pb2_grpc


class RaftCliente:

    def __init__(self):
        self.leader_addr: str | None = None  # endereço gRPC do líder atual

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

    def run(self):
        print("=" * 45)
        print("    Cliente Raft (gRPC)")
        print("=" * 45)
        print("Digite um comando e pressione Enter.")
        print("Digite 'sair' para encerrar.\n")

        while True:
            try:
                command = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n[CLIENTE] Encerrando.")
                break

            if command.lower() in ('sair', 'exit', 'quit'):
                break

            if not command:
                continue

            sucesso = self.send_command(command)

            # Retentar automaticamente se o cluster está em eleição
            retries = 0
            while not sucesso and retries < 5:
                retries += 1
                print(f"[CLIENTE] Aguardando líder... (tentativa {retries}/5)")
                time.sleep(2)
                sucesso = self.send_command(command)

            if sucesso:
                print(f"[CLIENTE] ✓ Comando '{command}' commitado com sucesso!\n")
            else:
                print(f"[CLIENTE] ✗ Falha ao enviar '{command}' após {retries} tentativas.\n")


if __name__ == "__main__":
    cliente = RaftCliente()
    cliente.run()