import sys

import Pyro5.api
from Pyro5.server import expose, Daemon
import config
import threading
import time
import random

class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.uri = config.NODES[node_id]
        self.state_machine = []

        self.current_term = 0
        self.voted_for = None
        self.role = 'follower'

        self.votes_received = 0
        self.leader_uri = None

        self.next_timeout_at = 0
        self._reset_election_timer()

        timer_thread = threading.Thread(target=self._watch_election_timeout, daemon=True)
        timer_thread.start()

    def _watch_election_timeout(self):
        while True:
            time.sleep(0.01)

            if self.role != 'leader':
                if time.time() > self.next_timeout_at:
                    self._start_election()

    def _start_election(self):
        self.role = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1
        self._reset_election_timer()

        for peer_id, uri in config.NODES.items():
            if peer_id != self.node_id:
                threading.Thread(target=self._ask_for_vote, args=(uri,)).start()

    def _ask_for_vote(self, uri):
        try:
            with Pyro5.api.Proxy(uri) as proxy:
                proxy._pyroTimeout = 0.1
                term, granted = proxy.request_vote(
                    self.current_term, self.node_id, 0, 0
                )

                if term > self.current_term:
                    self.current_term = term
                    self.role = 'follower'
                    self.voted_for = None
                    self._reset_election_timer()
                elif granted and self.role == 'candidate':
                    self._count_votes()
        except Exception:
            pass

    @expose
    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if term < self.current_term:
            return self.current_term, False

        if term > self.current_term:
            self.current_term = term
            self.role = 'follower'
            self.voted_for = None

        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            self._reset_election_timer()
            return self.current_term, True

        return self.current_term, False

    def _count_votes(self):
        self.votes_received += 1
        if self.votes_received > len(config.NODES)/2:
            if self.role != 'leader':
                self._become_leader()

    def _become_leader(self):
        self.role = 'leader'

        print(f"\n[NÓ {self.node_id}] LÍDER NO TERMO {self.current_term}")

        try:
            with Pyro5.api.locate_ns() as ns:
                ns.register(config.LEADER_NAME, self.uri)
        except Exception as e:
            print(e)

        threading.Thread(target=self._send_heartbeats, daemon=True).start()

    def _send_heartbeats(self):
        while self.role == 'leader':
            for peer_id, uri in config.NODES.items():
                if peer_id != self.node_id:
                    threading.Thread(target=self._send_append_entries, args=(uri,)).start()

            time.sleep(config.HEARTBEAT_INTERVAL)

    def _send_append_entries(self, uri):
        try:
            with Pyro5.api.Proxy(uri) as proxy:
                proxy._pyroTimeout = 0.1
                term, success = proxy.append_entries(
                    self.current_term, self.node_id, 0, 0, [], 0
                )

                if term > self.current_term:
                    self.current_term = term
                    self.role = 'follower'
                    self.voted_for = None
                    self._reset_election_timer()
        except Exception:
            pass

    @expose
    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if term < self.current_term:
            return self.current_term, False

        if term >= self.current_term:
            self.current_term = term
            self.role = 'follower'
            self.voted_for = None
            self._reset_election_timer()

        return self.current_term, True

    def _reset_election_timer(self):
        timeout = random.uniform(config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX)
        self.next_timeout_at = time.time() + timeout

    @expose
    def get_state(self):
        return {
            'node_id': self.node_id,
            'term': self.current_term,
            'role': self.role,
        }

    def _apply_to_state_machine(self, entry):
        self.state_machine.append(entry)
        for i in self.state_machine:
            print(i)

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