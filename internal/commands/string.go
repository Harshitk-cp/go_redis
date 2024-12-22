package commands

import (
	"go_redis/internal/datastore"
	"go_redis/internal/resp"

	"io"
	"strings"
)

func HandleStringCommands(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 2 {
		resp.EncodeError(conn, "ERR wrong number of arguments")
		return
	}

	switch strings.ToUpper(command[0]) {
	case "SET":
		handleSet(command, conn, ctx)
	case "GET":
		handleGet(command, conn, ctx)
	case "DEL":
		handleDel(command, conn, ctx)
	default:
		resp.EncodeError(conn, "ERR unknown command")
	}
}

func handleSet(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 3 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'SET'")
		return
	}

	key := command[1]
	value := command[2]
	db := ctx.DataStore.SelectDatabase(ctx.CurrentDB)
	db.SetWithLogging(ctx.DataStore, key, value, ctx.CurrentDB)

	resp.EncodeSimpleString(conn, "OK")
}

func handleGet(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 2 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'GET'")
		return
	}

	key := command[1]
	db := ctx.DataStore.SelectDatabase(ctx.CurrentDB)
	value, exists := db.Get(key)
	if !exists {
		resp.EncodeBulkString(conn, "")
		return
	}

	resp.EncodeBulkString(conn, value.(string))
}

func handleDel(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 2 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'DEL'")
		return
	}

	key := command[1]
	db := ctx.DataStore.SelectDatabase(ctx.CurrentDB)
	db.Delete(key)
	resp.EncodeInteger(conn, 1)
}
