import json
import os

from raft_log import LogEntry

STATES_DIR = "node_states"


class Persistence:

    def __init__(self, node_id: int):
        os.makedirs(STATES_DIR, exist_ok=True)
        self.path = os.path.join(STATES_DIR, f"node_{node_id}.json")
        self._tmp_path = self.path + ".tmp"

    def save(self, current_term: int, voted_for, log_entries: list, commit_index: int = 0):
        state = {
            "current_term": current_term,
            "voted_for": voted_for,
            "commit_index": commit_index,
            "log": [
                {
                    "index":   e.index,
                    "term":    e.term,
                    "command": e.command,
                }
                for e in log_entries
            ],
        }

        with open(self._tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(self._tmp_path, self.path)

    def load(self) -> dict | None:
        if not os.path.exists(self.path):
            return None

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[PERSISTÊNCIA] Aviso: erro ao ler estado — {e}. Iniciando do zero.")
            return None

    def restore_log(self, log_data: list) -> list[LogEntry]:
        return [
            LogEntry(
                index=entry["index"],
                term=entry["term"],
                command=entry["command"],
            )
            for entry in log_data
        ]

    def clear(self):
        for path in (self.path, self._tmp_path):
            if os.path.exists(path):
                os.remove(path)
