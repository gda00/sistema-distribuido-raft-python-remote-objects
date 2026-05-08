import subprocess
import time
import config

def start_nameserver():
    return subprocess.Popen(["python","-m","Pyro5.nameserver"])

def start_node(node_id):
    return subprocess.Popen(["python","raft_node.py",str(node_id)])

def main():
    ns_process = start_nameserver()
    time.sleep(2)

    node_processes = []
    for node_id in config.NODES.keys():
        proc = start_node(node_id)
        node_processes.append(proc)
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for proc in node_processes:
            proc.terminate()
        ns_process.terminate()

if __name__ == '__main__':
    main()