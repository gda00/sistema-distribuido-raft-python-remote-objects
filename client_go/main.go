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

var clusterNodes = []string{
	"localhost:50050",
	"localhost:50051",
	"localhost:50052",
	"localhost:50053",
}

type RaftClient struct {
	leaderAddr string
}

func newStub(addr string) (*grpc.ClientConn, pb.RaftServiceClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewRaftServiceClient(conn), nil
}

func (c *RaftClient) publish(command string) bool {
	const maxRetries = 5
	const retryDelay = 2 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if c.leaderAddr != "" {
			ok, hint := c.tryPublish(c.leaderAddr, command)
			if ok {
				return true
			}
			if hint != "" && hint != c.leaderAddr {
				c.leaderAddr = hint
				ok2, _ := c.tryPublish(c.leaderAddr, command)
				if ok2 {
					fmt.Printf("[CLIENTE] Redirecionado ao líder em %s\n", c.leaderAddr)
					return true
				}
			}
			c.leaderAddr = ""
		}

		for _, addr := range clusterNodes {
			ok, hint := c.tryPublish(addr, command)
			if ok {
				c.leaderAddr = addr
				fmt.Printf("[CLIENTE] Líder encontrado em %s\n", addr)
				return true
			}
			if hint != "" {
				ok2, _ := c.tryPublish(hint, command)
				if ok2 {
					c.leaderAddr = hint
					fmt.Printf("[CLIENTE] Líder encontrado (via hint) em %s\n", hint)
					return true
				}
			}
		}

		if attempt < maxRetries {
			fmt.Printf("[CLIENTE] Aguardando líder... (tentativa %d/%d)\n", attempt, maxRetries)
			time.Sleep(retryDelay)
		}
	}

	fmt.Println("[CLIENTE] ✗ Cluster indisponível após todas as tentativas.")
	return false
}

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

	if reply.LeaderHint != "" {
		c.leaderAddr = reply.LeaderHint
	}
}

func (c *RaftClient) chooseReadNode(requireLeader bool) string {
	if requireLeader {
		if c.leaderAddr != "" {
			return c.leaderAddr
		}
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
		return ""
	}

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
			break
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
