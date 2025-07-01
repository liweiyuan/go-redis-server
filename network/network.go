package network

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/liweiyuan/go-redis-server/command"
	"github.com/liweiyuan/go-redis-server/resp"
	"github.com/liweiyuan/go-redis-server/storage"
)

func Start(s *storage.Storage, cr *command.CommandRegistry) {
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	defer listener.Close()
	fmt.Println("Redis server listening on :6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn, s, cr)
	}
}

func handleConnection(conn net.Conn, s *storage.Storage, cr *command.CommandRegistry) {
	defer conn.Close()
	fmt.Printf("Accepted connection from %s\n", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		respValue, err := resp.ReadResp(reader)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("Error reading RESP: %v\n", err)
			}
			return
		}

		cmd, err := cr.ParseCommand(respValue)
		if err != nil {
			// If ParseCommand returns an error, it's already a RespValue error
			resp.WriteResp(writer, resp.NewError(err.Error()))
			writer.Flush()
			continue
		}

		result := cmd.Apply(s)
		err = resp.WriteResp(writer, result)
		if err != nil {
			fmt.Printf("Error writing RESP: %v\n", err)
			return
		}
		writer.Flush()
	}
}
