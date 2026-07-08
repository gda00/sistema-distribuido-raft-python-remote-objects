import grpc
import time
import config
import raft_pb2
import raft_pb2_grpc


class RaftCliente:

    def __init__(self):
        self.leader_addr: str | None = None

    def _try_node(self, addr: str, command: str) -> tuple[bool, str]:
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                req = raft_pb2.CommandRequest(command=command)
                reply = stub.ReceiveCommand(req, timeout=6.0)

                if reply.success:
                    return True, ''
                else:
                    return False, reply.leader_hint
        except grpc.RpcError:
            return False, ''
        except Exception:
            return False, ''

    def send_command(self, command: str) -> bool:
        if self.leader_addr:
            success, hint = self._try_node(self.leader_addr, command)
            if success:
                return True
            if hint and hint != self.leader_addr:
                self.leader_addr = hint
                success, _ = self._try_node(self.leader_addr, command)
                if success:
                    print(f"[CLIENTE] Reconectado ao líder em {self.leader_addr}")
                    return True
            self.leader_addr = None

        all_addrs = list(config.NODES.values())
        for addr in all_addrs:
            success, hint = self._try_node(addr, command)
            if success:
                self.leader_addr = addr
                print(f"[CLIENTE] Líder encontrado em {addr}")
                return True
            if hint and hint in all_addrs:
                success2, _ = self._try_node(hint, command)
                if success2:
                    self.leader_addr = hint
                    print(f"[CLIENTE] Líder encontrado (via hint) em {hint}")
                    return True

        print("[CLIENTE] Líder não encontrado. Cluster indisponível?")
        return False

    def read_data(self, addr: str) -> raft_pb2.ReadReply | None:
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
        if force_leader:
            return self._consume_from_leader()
        else:
            return self._consume_from_any()

    def _consume_from_any(self) -> bool:
        all_addrs = list(config.NODES.values())
        for addr in all_addrs:
            reply = self.read_data(addr)
            if reply and reply.success:
                self._print_entries(addr, reply)
                if reply.leader_hint:
                    self.leader_addr = reply.leader_hint
                return True

        print("[CLIENTE] ✗ Nenhum nó disponível para leitura.")
        return False

    def _consume_from_leader(self) -> bool:
        if self.leader_addr:
            reply = self.read_data(self.leader_addr)
            if reply and reply.success:
                self._print_entries(self.leader_addr, reply)
                return True
            self.leader_addr = None

        all_addrs = list(config.NODES.values())
        for addr in all_addrs:
            reply = self.read_data(addr)
            if reply and reply.success and reply.leader_hint:
                leader = reply.leader_hint
                self.leader_addr = leader
                leader_reply = self.read_data(leader)
                if leader_reply and leader_reply.success:
                    self._print_entries(leader, leader_reply)
                    return True

        print("[CLIENTE] ✗ Não foi possível determinar o líder para leitura forte.")
        return False

    def _print_entries(self, addr: str, reply: raft_pb2.ReadReply):
        print(f"\n[CLIENTE] Leitura de {addr} | commit_index={reply.commit_index}")
        print("─" * 45)
        if not reply.entries:
            print("  (nenhum dado committed ainda)")
        else:
            for i, entry in enumerate(reply.entries, start=1):
                print(f"  [{i}] {entry}")
        print("─" * 45)

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