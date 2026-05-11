import Pyro5.api
from Pyro5.server import expose
from raft_log import LogEntry
import config
import threading

class Replication:

    @expose
    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if term < self.current_term:
            return self.current_term, False

        if term >= self.current_term:
            self.current_term = term
            self.role = 'follower'
            self.voted_for = None
            self._reset_election_timer()

        if not self.log.match_entry(prev_log_index, prev_log_term):
            return self.current_term, False
        
        for entry_data in entries:
            if isinstance(entry_data, dict):
                idx = entry_data.get('index')
                trm = entry_data.get('term')
                cmd = entry_data.get('command')
            else:
                idx = entry_data.index               
                trm = entry_data.term          
                cmd = entry_data.command

            existing_entry = self.log.get_entry(idx)

            if existing_entry and existing_entry.term != trm:
                self.log.delete_from(idx)
                existing_entry = None
            
            if not existing_entry:
                self.log.append(LogEntry(idx, trm, cmd))
        
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.get_last_index())
            self._apply_commits()

        return self.current_term, True
    
    @expose
    def receive_command(self, command):
        if self.role != 'leader':
            raise Exception("Não sou o líder! :p")
        
        entry =  LogEntry(self.log.get_last_index() + 1, self.current_term, command)
        self.log.append(entry)
        print(f"[LÍDER] comando recebido: '{command}' -> replicando...")

        self._replicate_to_followers()
        return True
    
    def _replicate_to_followers(self):
        for peer_id, uri in config.NODES.items():
            if peer_id != self.node_id:
                threading.Thread(target=self._send_to_peer, args=(peer_id, uri)).start()

    def _send_to_peer(self, peer_id, uri):
        if self.role != 'leader': return

        next_idx = self.next_index[peer_id]
        prev_log_index = next_idx -1

        prev_entry = self.log.get_entry(prev_log_index)
        prev_log_term = prev_entry.term if prev_entry else 0

        entries = self.log.get_entries_from(next_idx)
        entries_dicts = [{'index': e.index, 'term': e.term, 'command': e.command} for e in entries]

        try:
             with Pyro5.api.Proxy(uri) as proxy:
                proxy._pyroTimeout = 2.0
                term, success = proxy.append_entries(
                    self.current_term, self.node_id, prev_log_index, prev_log_term, entries_dicts, self.commit_index               
                )

                if term > self.current_term:
                    self.current_term = term
                    self.role = 'follower'
                    self.voted_for = None
                    self._reset_election_timer()
                    return
                
                if success:
                    if entries:
                        self.next_index[peer_id] = entries[-1].index + 1
                        self.match_index[peer_id] = entries[-1].index
                        self._check_commit()
                else:
                    self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)
        except Exception:
            pass
            #print(f"Erro RPC enviando para {peer_id}: {Exception}")

    def _check_commit(self):
        for N in range(self.commit_index + 1, self.log.get_last_index() + 1):
            match_count = 1 + sum(1 for match_idx in self.match_index.values() if match_idx >= N)
            
            if match_count > len(config.NODES) / 2:
                if self.log.get_entry(N).term == self.current_term:
                    self.commit_index = N
                    self._apply_commits()


    def _apply_commits(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get_entry(self.last_applied)
            if entry and entry.command:
                self._apply_to_state_machine(f"[COMMIT] índice {entry.index} * termo {entry.term} * comando: {entry.command}")
