"""
persistence.py — Persistência de estado do nó Raft em disco

O paper do Raft define que três variáveis DEVEM ser persistidas antes de
responder qualquer RPC (para sobreviver a falhas/reinicializações):
  1. current_term — evita votar em dois candidatos no mesmo termo
  2. voted_for    — evita conceder dois votos no mesmo termo
  3. log[]        — não perder entradas já aceitas

Além das três variáveis do paper, este projeto também persiste:
  4. commit_index — permite que o nó saiba, após recuperação, quais
                    entradas já foram efetivadas (committed), sem precisar
                    aguardar heartbeat do líder. Atende ao requisito do
                    enunciado: "Dados finais (committed)" devem ser persistidos.

Variáveis VOLÁTEIS (não precisam de persistência):
  - role         → sempre reinicia como 'follower'
  - next_index, match_index → só o líder usa, reinicializa ao se tornar líder

Estratégia de escrita: "write-then-rename" (atômica no SO)
  Grava em arquivo temporário e renomeia. Isso garante que, se o processo
  morrer no meio da escrita, o arquivo antigo permanece íntegro.
"""
import json
import os

from raft_log import LogEntry

# Diretório onde os arquivos de estado serão gravados
STATES_DIR = "node_states"


class Persistence:

    def __init__(self, node_id: int):
        os.makedirs(STATES_DIR, exist_ok=True)
        self.path = os.path.join(STATES_DIR, f"node_{node_id}.json")
        self._tmp_path = self.path + ".tmp"

    # ------------------------------------------------------------------
    # SALVAR
    # ------------------------------------------------------------------

    def save(self, current_term: int, voted_for, log_entries: list, commit_index: int = 0):
        """
        Persiste o estado crítico do nó em disco.

        Deve ser chamado ANTES de enviar qualquer resposta RPC que dependa
        das variáveis persistidas (conforme §5.4.1 do paper do Raft).

        Parâmetros:
            current_term : int  — termo atual do nó
            voted_for    : int | None — ID do candidato votado neste termo
            log_entries  : list[LogEntry] — todas as entradas do log (incluindo sentinela)
            commit_index : int  — índice da última entrada committed (padrão 0)
        """
        state = {
            "current_term": current_term,
            "voted_for": voted_for,                   # None é serializado como null em JSON
            "commit_index": commit_index,              # persiste dados efetivados (committed)
            "log": [
                {
                    "index":   e.index,
                    "term":    e.term,
                    "command": e.command,              # None para a entrada sentinela (índice 0)
                }
                for e in log_entries
            ],
        }

        # Escrita atômica: grava no .tmp e depois renomeia
        with open(self._tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(self._tmp_path, self.path)  # operação atômica no Windows e Linux

    # ------------------------------------------------------------------
    # CARREGAR
    # ------------------------------------------------------------------

    def load(self) -> dict | None:
        """
        Carrega o estado persistido do disco.

        Retorna um dicionário com as chaves:
          'current_term', 'voted_for', 'commit_index' e 'log',
        ou None se não houver arquivo de estado (primeira inicialização).
        """
        if not os.path.exists(self.path):
            return None

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"[PERSISTÊNCIA] Aviso: erro ao ler estado — {e}. Iniciando do zero.")
            return None

    def restore_log(self, log_data: list) -> list[LogEntry]:
        """
        Converte a lista de dicts do JSON de volta para objetos LogEntry.

        Parâmetro:
            log_data : list[dict] — lista vinda do JSON ('log' key)
        """
        return [
            LogEntry(
                index=entry["index"],
                term=entry["term"],
                command=entry["command"],   # pode ser None para a sentinela
            )
            for entry in log_data
        ]

    # ------------------------------------------------------------------
    # LIMPAR (útil nos testes)
    # ------------------------------------------------------------------

    def clear(self):
        """Remove o arquivo de estado (útil para testes ou reset do cluster)."""
        for path in (self.path, self._tmp_path):
            if os.path.exists(path):
                os.remove(path)
