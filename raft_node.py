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

    def __init__(self, node_id: int):
        self.node_id = node_id
        self.addr = config.NODES[node_id]
        self.state_machine = []

        self._persistence = Persistence(node_id)

        self.current_term = 0
        self.voted_for = None

        self.log = RaftLog()

        self.role = 'follower'
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0

        self._load_persisted_state()

        self.next_index = {}
        self.match_index = {}

        self.votes_received = 0

        self._state_lock = threading.Lock()
        self._commit_condition = threading.Condition()

        self.next_timeout_at = 0
        self._reset_election_timer()

        threading.Thread(target=self._watch_election_timeout, daemon=True).start()

    def _save_state(self):
        self._persistence.save(
            current_term=self.current_term,
            voted_for=self.voted_for,
            log_entries=self.log.entries,
            commit_index=self.commit_index,
        )

    def _load_persisted_state(self):
        state = self._persistence.load()
        if state is None:
            print(f"[NÓ {self.node_id}] Primeira inicialização — sem estado persistido.")
            return

        self.current_term = state["current_term"]
        self.voted_for    = state["voted_for"]
        self.log.entries  = self._persistence.restore_log(state["log"])

        self.commit_index = state.get("commit_index", 0)

        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get_entry(self.last_applied)
            if entry and entry.command:
                self.state_machine.append(entry.command)

        print(
            f"[NÓ {self.node_id}] Estado restaurado do disco: "
            f"term={self.current_term}, voted_for={self.voted_for}, "
            f"log_size={self.log.get_last_index()}, commit_index={self.commit_index}"
        )

    def _reset_election_timer(self):
        timeout = random.uniform(config.ELECTION_TIMEOUT_MIN, config.ELECTION_TIMEOUT_MAX)
        self.next_timeout_at = time.time() + timeout

    def _watch_election_timeout(self):
        while True:
            time.sleep(0.01)
            if self.role != 'leader' and time.time() > self.next_timeout_at:
                self._start_election()

    def _start_election(self):
        with self._state_lock:
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = 1
            self.leader_id = None
            self._reset_election_timer()
            self._save_state()

        print(f"[NÓ {self.node_id}] Iniciando eleição → TERMO {self.current_term}")

        for peer_id, addr in config.NODES.items():
            if peer_id != self.node_id:
                threading.Thread(
                    target=self._ask_for_vote,
                    args=(peer_id, addr),
                    daemon=True
                ).start()

    def _ask_for_vote(self, peer_id: int, addr: str):
        try:
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

            if reply.term > self.current_term:
                with self._state_lock:
                    self.current_term = reply.term
                    self.role = 'follower'
                    self.voted_for = None
                    self._reset_election_timer()

            elif reply.vote_granted and self.role == 'candidate' and term_snapshot == self.current_term:
                self._count_votes()

        except Exception:
            pass

    def _count_votes(self):
        with self._state_lock:
            self.votes_received += 1
            majority = len(config.NODES) / 2
            if self.votes_received > majority and self.role != 'leader':
                self._become_leader()

    def _become_leader(self):
        self.role = 'leader'
        self.leader_id = self.node_id

        self.next_index = {
            p: self.log.get_last_index() + 1
            for p in config.NODES if p != self.node_id
        }
        self.match_index = {p: 0 for p in config.NODES if p != self.node_id}

        print(f"\n{'='*50}")
        print(f"[NÓ {self.node_id}] *** ELEITO LÍDER NO TERMO {self.current_term} ***")
        print(f"{'='*50}\n")

        threading.Thread(target=self._send_heartbeats, daemon=True).start()

    def _send_heartbeats(self):
        while self.role == 'leader':
            self._replicate_to_followers()
            time.sleep(config.HEARTBEAT_INTERVAL)

    def RequestVote(self, request, context):
        term = request.term
        candidate_id = request.candidate_id
        last_log_index = request.last_log_index
        last_log_term = request.last_log_term

        if term < self.current_term:
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=False)

        my_last_term = self.log.get_last_term()
        my_last_index = self.log.get_last_index()
        log_ok = (last_log_term > my_last_term) or \
                 (last_log_term == my_last_term and last_log_index >= my_last_index)

        if term > self.current_term:
            self.current_term = term
            self.role = 'follower'
            self.voted_for = None
            self._save_state()

        if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            self.voted_for = candidate_id
            self._reset_election_timer()
            self._save_state()
            return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=True)

        return raft_pb2.RequestVoteReply(term=self.current_term, vote_granted=False)

    def _apply_to_state_machine(self, entry: str):
        self.state_machine.append(entry)
        print(entry)

    def get_state(self) -> dict:
        return {
            'node_id': self.node_id,
            'term': self.current_term,
            'role': self.role,
            'leader_id': self.leader_id,
            'log_size': self.log.get_last_index(),
            'commit_index': self.commit_index,
        }


def main():
    if len(sys.argv) < 2:
        print("Uso: python raft_node.py <node_id>")
        sys.exit(1)

    node_id = int(sys.argv[1])
    addr = config.NODES[node_id]

    raft_node = RaftNode(node_id)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_node, server)
    server.add_insecure_port(addr)
    server.start()

    print(f"[NÓ {node_id}] Servidor gRPC iniciado em {addr}")
    server.wait_for_termination()


if __name__ == '__main__':
    main()