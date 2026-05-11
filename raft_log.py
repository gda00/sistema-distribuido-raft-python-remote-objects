class LogEntry:
    def __init__(self, index, term, command):
        self.index = index
        self.term = term
        self.command = command

    def __repr__(self):
        return f"LogEntry(index={self.index}, term={self.term}, command = '{self.command}')"
    
class RaftLog:

    def __init__(self):
        self.entries = [LogEntry(0, 0, None)]
        self.commit_index = 0
        self.last_applied = 0

    def append(self, entry):
        self.entries.append(entry)

    def get_last_index(self):
        return len(self.entries) -1
    
    def get_last_term(self):
        return self.entries[-1].term
    
    def get_entry(self, index):
        if 0 <= index < len(self.entries):
            return self.entries[index]
        return None
    
    def get_entries_from(self, index):
        if index < len(self.entries):
            return self.entries[index:]
        return []
    
    def match_entry(self, index, term):
        if index>= len(self.entries) or index < 0:
            return False
        
        return self.entries[index].term == term
    
    def delete_from(self, index):
        if index < len(self.entries):
            self.entries = self.entries[:index]
