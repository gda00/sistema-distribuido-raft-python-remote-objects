/*
main.go — Cliente Raft externo em Go

Cliente interativo que se conecta ao cluster Raft via gRPC e permite:
  - publish <dado>  : envia um comando ao cluster (write, via ReceiveCommand)
  - consume         : lê todos os dados committed do nó atual (via ReadData)
  - consume --leader: garante leitura do líder (leitura forte)
  - help            : exibe ajuda
  - quit / exit     : encerra o cliente

Estratégia de descoberta do líder:
  1. Tenta o último líder conhecido.
  2. Se falhar ou receber leader_hint, redireciona automaticamente.
  3. Se tudo falhar, varre todos os nós em round-robin.
  4. Retry automático com backoff em caso de eleição em andamento.

Restrição do enunciado: o cliente SÓ usa ReceiveCommand e ReadData.
Nunca chama RequestVote ou AppendEntries (operações internas do Raft).
*/
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/breba/raft-client/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Endereços dos nós do cluster Raft (deve bater com config.py do servidor Python)
var clusterNodes = []string{
	"localhost:50050",
	"localhost:50051",
	"localhost:50052",
	"localhost:50053",
}

// RaftClient mantém o estado do cliente: endereço do líder e conexão atual.
type RaftClient struct {
	leaderAddr string // endereço gRPC do líder conhecido (pode ser vazio)
}

// newStub abre uma conexão gRPC para o endereço dado e retorna o stub.
// O caller é responsável por fechar a conexão com defer conn.Close().
func newStub(addr string) (*grpc.ClientConn, pb.RaftServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewRaftServiceClient(conn), nil
}

// publish envia um comando (write) ao cluster.
// Descobre o líder automaticamente via leader_hint ou varrendo todos os nós.
// Faz até maxRetries tentativas em caso de eleição em andamento.
func (c *RaftClient) publish(command string) bool {
	const maxRetries = 5
	const retryDelay = 2 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// 1. Tenta o líder conhecido primeiro
		if c.leaderAddr != "" {
			ok, hint := c.tryPublish(c.leaderAddr, command)
			if ok {
				return true
			}
			if hint != "" && hint != c.leaderAddr {
				// Redirecionamento direto ao líder indicado
				c.leaderAddr = hint
				ok2, _ := c.tryPublish(c.leaderAddr, command)
				if ok2 {
					fmt.Printf("[CLIENTE] Redirecionado ao líder em %s\n", c.leaderAddr)
					return true
				}
			}
			c.leaderAddr = "" // líder anterior inválido
		}

		// 2. Varre todos os nós em busca do líder
		for _, addr := range clusterNodes {
			ok, hint := c.tryPublish(addr, command)
			if ok {
				c.leaderAddr = addr
				fmt.Printf("[CLIENTE] Líder encontrado em %s\n", addr)
				return true
			}
			if hint != "" {
				// Vai direto ao líder sugerido
				ok2, _ := c.tryPublish(hint, command)
				if ok2 {
					c.leaderAddr = hint
					fmt.Printf("[CLIENTE] Líder encontrado (via hint) em %s\n", hint)
					return true
				}
			}
		}

		// 3. Nenhum nó aceitou — provavelmente há eleição em andamento
		if attempt < maxRetries {
			fmt.Printf("[CLIENTE] Aguardando líder... (tentativa %d/%d)\n", attempt, maxRetries)
			time.Sleep(retryDelay)
		}
	}

	fmt.Println("[CLIENTE] ✗ Cluster indisponível após todas as tentativas.")
	return false
}

// tryPublish tenta enviar um comando a um nó específico.
// Retorna (sucesso, leader_hint).
func (c *RaftClient) tryPublish(addr, command string) (bool, string) {
	conn, stub, err := newStub(addr)
	if err != nil {
		return false, ""
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()

	reply, err := stub.ReceiveCommand(ctx, &pb.CommandRequest{Command: command})
	if err != nil {
		return false, ""
	}
	if reply.Success {
		return true, ""
	}
	return false, reply.LeaderHint
}

// consume lê todos os dados committed de um nó do cluster.
// Se requireLeader=true, garante que a leitura vem do líder (leitura forte).
// Se requireLeader=false, pode ler de qualquer réplica (consistência eventual).
func (c *RaftClient) consume(requireLeader bool) {
	addr := c.chooseReadNode(requireLeader)
	if addr == "" {
		fmt.Println("[CLIENTE] ✗ Nenhum nó disponível para leitura.")
		return
	}

	conn, stub, err := newStub(addr)
	if err != nil {
		fmt.Printf("[CLIENTE] ✗ Erro ao conectar a %s: %v\n", addr, err)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := stub.ReadData(ctx, &pb.ReadRequest{})
	if err != nil {
		fmt.Printf("[CLIENTE] ✗ Erro ao ler dados de %s: %v\n", addr, err)
		return
	}

	if !reply.Success {
		fmt.Println("[CLIENTE] ✗ Nó respondeu sem sucesso.")
		return
	}

	fmt.Printf("\n[CLIENTE] Leitura de %s | commit_index=%d\n", addr, reply.CommitIndex)
	fmt.Println("─────────────────────────────────────────")

	if len(reply.Entries) == 0 {
		fmt.Println("  (nenhum dado committed ainda)")
	} else {
		for i, entry := range reply.Entries {
			fmt.Printf("  [%d] %s\n", i+1, entry)
		}
	}
	fmt.Println("─────────────────────────────────────────")

	// Atualiza o líder conhecido se o nó informar
	if reply.LeaderHint != "" {
		c.leaderAddr = reply.LeaderHint
	}
}

// chooseReadNode seleciona o nó para leitura.
// Se requireLeader=true, vai ao líder (ou descobre qual é).
// Se requireLeader=false, usa round-robin nos nós disponíveis.
func (c *RaftClient) chooseReadNode(requireLeader bool) string {
	if requireLeader {
		// Tenta o líder conhecido; se não souber, faz uma query rápida
		if c.leaderAddr != "" {
			return c.leaderAddr
		}
		// Descobre o líder fazendo uma leitura em qualquer nó e usando o hint
		for _, addr := range clusterNodes {
			conn, stub, err := newStub(addr)
			if err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			reply, err := stub.ReadData(ctx, &pb.ReadRequest{})
			cancel()
			conn.Close()
			if err == nil && reply.LeaderHint != "" {
				c.leaderAddr = reply.LeaderHint
				return c.leaderAddr
			}
		}
		return "" // não foi possível descobrir o líder
	}

	// Leitura eventual: qualquer nó disponível
	for _, addr := range clusterNodes {
		conn, stub, err := newStub(addr)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err = stub.ReadData(ctx, &pb.ReadRequest{})
		cancel()
		conn.Close()
		if err == nil {
			return addr
		}
	}
	return ""
}

func printHelp() {
	fmt.Println()
	fmt.Println("Comandos disponíveis:")
	fmt.Println("  publish <dado>     — envia um dado ao cluster Raft (write)")
	fmt.Println("  consume            — lê dados committed de qualquer nó (consistência eventual)")
	fmt.Println("  consume --leader   — lê dados committed do líder (leitura forte)")
	fmt.Println("  help               — exibe esta ajuda")
	fmt.Println("  quit / exit        — encerra o cliente")
	fmt.Println()
}

func main() {
	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║      Cliente Raft — Go (gRPC)            ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Printf("Cluster: %v\n", clusterNodes)
	printHelp()

	client := &RaftClient{}
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break // EOF ou Ctrl+C
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "publish":
			if len(parts) < 2 {
				fmt.Println("[CLIENTE] Uso: publish <dado>")
				continue
			}
			data := strings.Join(parts[1:], " ")
			if client.publish(data) {
				fmt.Printf("[CLIENTE] ✓ '%s' publicado com sucesso!\n\n", data)
			}

		case "consume":
			requireLeader := len(parts) > 1 && parts[1] == "--leader"
			client.consume(requireLeader)

		case "help":
			printHelp()

		case "quit", "exit", "sair":
			fmt.Println("[CLIENTE] Encerrando.")
			return

		default:
			fmt.Printf("[CLIENTE] Comando desconhecido: '%s'. Digite 'help' para ajuda.\n", cmd)
		}
	}
}
