NODES = {
    0: "PYRO:peerA@locahost:6767",
    1: "PYRO:peerB@locahost:6768",
    2: "PYRO:peerC@locahost:6769",
    3: "PYRO:peerD@locahost:6770"
}

LEADER_NAME = "Leader"

HEARTBEAT_INTERVAL = 0.05 # 50ms

ELECTION_TIMEOUT_MIN = 0.15 # 150ms
ELECTION_TIMEOUT_MAX = 0.30 # 300ms