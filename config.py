# Mapeamento de ID do nó para endereço gRPC (host:porta)
# Cada nó escuta em uma porta diferente no localhost
NODES = {
    0: "localhost:50050",
    1: "localhost:50051",
    2: "localhost:50052",
    3: "localhost:50053",
}

# Intervalo entre heartbeats enviados pelo líder (em segundos)
HEARTBEAT_INTERVAL = 0.5  # meio segundo

# Intervalo aleatório de timeout de eleição (em segundos)
# Se um seguidor não receber heartbeat nesse prazo, inicia eleição
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0