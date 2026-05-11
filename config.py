NODES = {
    0: "PYRO:peerA@localhost:6767",
    1: "PYRO:peerB@localhost:6768",
    2: "PYRO:peerC@localhost:6769",
    3: "PYRO:peerD@localhost:6770"
}

LEADER_NAME = "Leader"

HEARTBEAT_INTERVAL = 0.5 # meio segundo

ELECTION_TIMEOUT_MIN = 1.5 # 1.5 segundos
ELECTION_TIMEOUT_MAX = 3.0 # 3 segundos