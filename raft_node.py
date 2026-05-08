import sys
from Pyro5.server import expose, Daemon
import config
import threading
import time
import random

@expose
class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.uri = config.NODES[node_id]

        self.current_term = 0
        self.voted_for = None
        self.role = 'follower'

        self.votes_received = 0
        self.leader_uri = None

        self.next_timeout_at = 0
        self._reset_election_timer()

        timer_thread = threading.Thread(target=self._watch_election_timeout, daemon=True)
        timer_thread.start()

    def _reset_election_timer(self):
        timeout = random.uniform(config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX)
        self.next_timeout_at = time.time() + timeout

    def _watch_election_timeout(self):
        while True:
            time.sleep(0.01)

            if self.role != 'leader':
                if time.time() > self.next_timeout_at:
                    self._start_election()

    def get_state(self):
        return {
            'node_id': self.node_id,
            'term': self.current_term,
            'role': self.role,
        }

    def _start_election(self):
        self.role = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1
        self._reset_election_timer()

def main():
    node_id = int(sys.argv[1])
    raft_node = RaftNode(node_id)

    uri = config.NODES[node_id]
    parts = uri.replace("PYRO:","").replace("@",":").split(":")

    object_id = parts[0]
    host = parts[1]
    port = int(parts[2])

    daemon = Daemon(host=host, port=port)
    daemon.register(raft_node, objectId=object_id)
    daemon.requestLoop()

if __name__ == '__main__':
    main()