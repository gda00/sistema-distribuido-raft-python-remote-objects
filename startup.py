"""
startup.py — Inicializador do cluster Raft com gRPC

Inicia todos os nós do cluster em subprocessos separados.
Permite derrubar nós individuais pelo terminal para simular falhas.
"""
import subprocess
import sys
import time
import config


def start_node(node_id: int) -> subprocess.Popen:
    """Inicia um nó Raft em um subprocesso separado."""
    import os
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return subprocess.Popen([sys.executable, "raft_node.py", str(node_id)], env=env)


def main():
    print("=== Iniciando cluster Raft (gRPC) ===")
    print(f"Nós configurados: {list(config.NODES.keys())}")
    print()

    node_processes: list[subprocess.Popen] = []

    for node_id in config.NODES.keys():
        addr = config.NODES[node_id]
        proc = start_node(node_id)
        node_processes.append(proc)
        print(f"[OK] Nó {node_id} iniciado em {addr} (PID: {proc.pid})")
        time.sleep(0.2)  # pequeno delay entre inicializações

    print()
    print("Cluster iniciado! Comandos disponíveis:")
    print("  kill <ID>    — derruba o nó com o ID especificado")
    print("  restart <ID> — reinicia o nó com o ID especificado")
    print("  status       — mostra quais nós estão ativos")
    print("  Ctrl+C       — encerra todo o cluster")
    print()

    try:
        while True:
            cmd = input("> ").strip()

            if not cmd:
                continue

            parts = cmd.split()
            action = parts[0].lower()

            if action == "kill" and len(parts) == 2:
                n_id = int(parts[1])
                if node_processes[n_id].poll() is None:
                    node_processes[n_id].terminate()
                    print(f"[!] Nó {n_id} finalizado.")
                else:
                    print(f"[!] Nó {n_id} já estava parado.")

            elif action == "restart" and len(parts) == 2:
                n_id = int(parts[1])
                if node_processes[n_id].poll() is None:
                    node_processes[n_id].terminate()
                    time.sleep(0.5)
                proc = start_node(n_id)
                node_processes[n_id] = proc
                print(f"[OK] Nó {n_id} reiniciado (PID: {proc.pid})")

            elif action == "status":
                for nid, proc in enumerate(node_processes):
                    status = "ATIVO" if proc.poll() is None else "PARADO"
                    addr = config.NODES[nid]
                    print(f"  Nó {nid} ({addr}): {status}")

            else:
                print("Comando inválido. Use: kill <ID> | restart <ID> | status")

    except KeyboardInterrupt:
        print("\n[!] Encerrando cluster...")
        for proc in node_processes:
            if proc.poll() is None:
                proc.terminate()
        print("[OK] Todos os nós finalizados.")


if __name__ == '__main__':
    main()