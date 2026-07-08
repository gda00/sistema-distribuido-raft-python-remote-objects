import grpc
import raft_pb2
import raft_pb2_grpc
from raft_log import LogEntry
import config
import threading
import time


class Replication:

    def AppendEntries(self, request, context):
        term = request.term
        leader_id = request.leader_id
        prev_log_index = request.prev_log_index
        prev_log_term = request.prev_log_term
        entries = request.entries
        leader_commit = request.leader_commit

        if term < self.current_term:
            return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)

        term_changed = term > self.current_term
        if term >= self.current_term:
            self.current_term = term
            self.role = 'follower'
            self.voted_for = None
            self.leader_id = leader_id
            self._reset_election_timer()

        if term_changed:
            self._save_state()

        if not self.log.match_entry(prev_log_index, prev_log_term):
            return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)

        log_changed = False
        for entry_data in entries:
            idx = entry_data.index
            trm = entry_data.term
            cmd = entry_data.command

            existing = self.log.get_entry(idx)

            if existing and existing.term != trm:
                self.log.delete_from(idx)
                existing = None
                log_changed = True

            if not existing:
                self.log.append(LogEntry(idx, trm, cmd))
                log_changed = True

        if log_changed:
            self._save_state()

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.get_last_index())
            self._apply_commits()

        return raft_pb2.AppendEntriesReply(term=self.current_term, success=True)

    def ReadData(self, request, context):
        committed_entries = [
            entry.command
            for entry in self.log.entries[1:self.commit_index + 1]
            if entry.command is not None
        ]

        leader_addr = ''
        if self.leader_id is not None:
            leader_addr = config.NODES.get(self.leader_id, '')

        return raft_pb2.ReadReply(
            success=True,
            entries=committed_entries,
            commit_index=self.commit_index,
            leader_hint=leader_addr,
        )

    def ReceiveCommand(self, request, context):
        if self.role != 'leader':
            leader_addr = config.NODES.get(self.leader_id, '') if self.leader_id is not None else ''
            return raft_pb2.CommandReply(success=False, leader_hint=leader_addr)

        command = request.command
        new_index = self.log.get_last_index() + 1
        entry = LogEntry(new_index, self.current_term, command)
        self.log.append(entry)
        self._save_state()
        print(f"[LÍDER {self.node_id}] Comando recebido: '{command}' (índice {new_index}) → replicando...")

        self._replicate_to_followers()

        deadline = time.time() + 5.0
        with self._commit_condition:
            while self.commit_index < new_index:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return raft_pb2.CommandReply(success=False, leader_hint='')
                if self.role != 'leader':
                    return raft_pb2.CommandReply(success=False, leader_hint='')
                self._commit_condition.wait(timeout=min(remaining, 0.1))

        return raft_pb2.CommandReply(success=True)

    def _replicate_to_followers(self):
        for peer_id, addr in config.NODES.items():
            if peer_id != self.node_id:
                threading.Thread(
                    target=self._send_to_peer,
                    args=(peer_id, addr),
                    daemon=True
                ).start()

    def _send_to_peer(self, peer_id: int, addr: str):
        if self.role != 'leader':
            return

        next_idx = self.next_index[peer_id]
        prev_log_index = next_idx - 1

        prev_entry = self.log.get_entry(prev_log_index)
        prev_log_term = prev_entry.term if prev_entry else 0

        local_entries = self.log.get_entries_from(next_idx)

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

            if reply.term > self.current_term:
                self.current_term = reply.term
                self.role = 'follower'
                self.voted_for = None
                self._reset_election_timer()
                return

            if reply.success:
                if local_entries:
                    self.next_index[peer_id] = local_entries[-1].index + 1
                    self.match_index[peer_id] = local_entries[-1].index
                    self._check_commit()
            else:
                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)

        except Exception:
            pass

    def _check_commit(self):
        for N in range(self.commit_index + 1, self.log.get_last_index() + 1):
            replicated = 1 + sum(
                1 for idx in self.match_index.values() if idx >= N
            )
            majority = len(config.NODES) / 2

            if replicated > majority and self.log.get_entry(N).term == self.current_term:
                self.commit_index = N
                self._apply_commits()

    def _apply_commits(self):
        applied_any = False
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get_entry(self.last_applied)
            if entry and entry.command:
                self._apply_to_state_machine(
                    f"[COMMIT] índice {entry.index} | termo {entry.term} | comando: '{entry.command}'"
                )
                applied_any = True

        if applied_any:
            self._save_state()

        with self._commit_condition:
            self._commit_condition.notify_all()
