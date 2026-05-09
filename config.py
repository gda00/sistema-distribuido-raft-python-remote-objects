NODES = {
    0: "PYRO:peerA@localhost:6767",
    1: "PYRO:peerB@localhost:6768",
    2: "PYRO:peerC@localhost:6769",
    3: "PYRO:peerD@localhost:6770"
}

LEADER_NAME = "Leader"

HEARTBEAT_INTERVAL = 0.010 # 10ms

ELECTION_TIMEOUT_MIN = 0.15 # 150ms
ELECTION_TIMEOUT_MAX = 0.30 # 300ms