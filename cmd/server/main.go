package main

import (
	"fmt"
	"go_redis/internal/commands"
	"go_redis/internal/datastore"
	"go_redis/internal/resp"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

func main() {

	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
	defer listener.Close()

	ctx := datastore.NewConnection()

	if err := ctx.DataStore.Recover("snapshot.rdb", "appendonly.aof"); err != nil {
		log.Printf("Error during recovery: %v", err)
	}

	ctx.DataStore.StartSnapshotRoutine()

	log.Println("GoRedis server is running at port 6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection error: %v", err)
			continue
		}
		go handleConnection(conn, ctx)
	}

}

func handleConnection(conn net.Conn, ctx *datastore.ConnectionContext) {
	defer conn.Close()
	for {

		command, err := resp.Parse(conn)
		if err != nil {
			log.Printf("Error parsing command: %v", err)
			return
		}
		execute(command, conn, ctx)
	}
}

func execute(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) == 0 {
		conn.Write([]byte("-ERR unknown command\r\n"))
		return
	}

	switch strings.ToUpper(command[0]) {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		if len(command) < 2 {
			conn.Write([]byte("-ERR wrong number of arguments\r\n"))
		} else {
			conn.Write([]byte(fmt.Sprintf("+%s\r\n", command[1])))
		}

	case "SET", "GET", "DEL":
		commands.HandleStringCommands(command, conn, ctx)
	case "SELECT":
		handleSelect(command, conn, ctx)
	case "SAVE":
		handleSave(command, conn, ctx.DataStore)
	default:
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}

func handleSelect(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 2 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'SELECT'")
		return
	}

	dbIndex, err := strconv.Atoi(command[1])
	if err != nil || dbIndex < 0 {
		resp.EncodeError(conn, "ERR invalid database index")
		return
	}

	ctx.CurrentDB = dbIndex
	resp.EncodeSimpleString(conn, "OK")
}

func handleSave(command []string, conn io.Writer, store *datastore.DataStore) {
	if len(command) < 3 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'SAVE'")
		return
	}

	interval, err1 := strconv.Atoi(command[1])
	threshold, err2 := strconv.Atoi(command[2])
	filepath := command[3]
	if err1 != nil || err2 != nil || interval <= 0 || threshold <= 0 {
		resp.EncodeError(conn, "ERR invalid arguments for 'SAVE'")
		return
	}

	store.UpdateRDBConfig(time.Duration(interval)*time.Second, filepath)
	resp.EncodeSimpleString(conn, "OK")
}
