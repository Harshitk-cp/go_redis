package commands

import (
	"go_redis/internal/datastore"
	"go_redis/internal/resp"

	"io"
	"strconv"
	"strings"
)

func HandleListCommands(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 2 {
		resp.EncodeError(conn, "ERR wrong number of arguments")
		return
	}

	switch strings.ToUpper(command[0]) {
	case "LPUSH":
		handleLPush(command, conn, ctx)
	case "RPUSH":
		handleRPush(command, conn, ctx)
	case "LPOP":
		handleLPop(command, conn, ctx)
	case "RPOP":
		handleRPop(command, conn, ctx)
	case "LRANGE":
		handleLRange(command, conn, ctx)
	default:
		resp.EncodeError(conn, "ERR unknown command")
	}
}

func handleLPush(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 3 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'LPUSH'")
		return
	}

	key := command[1]
	values := command[2:]
	db := ctx.DataStore.SelectDatabase(ctx.CurrentDB)
	db.LPushWithLogging(ctx.DataStore, key, values, ctx.CurrentDB)

	resp.EncodeInteger(conn, len(values))
}

func handleRPush(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 3 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'RPUSH'")
		return
	}

	key := command[1]
	values := command[2:]
	db := ctx.DataStore.SelectDatabase(ctx.CurrentDB)
	db.RPushWithLogging(ctx.DataStore, key, values, ctx.CurrentDB)

	resp.EncodeInteger(conn, len(values))
}

func handleLPop(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 2 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'LPOP'")
		return
	}

	key := command[1]
	db := ctx.DataStore.SelectDatabase(ctx.CurrentDB)
	value, exists := db.LPop(key)
	if !exists {
		resp.EncodeBulkString(conn, "")
		return
	}

	resp.EncodeBulkString(conn, value)
}

func handleRPop(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 2 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'RPOP'")
		return
	}

	key := command[1]
	db := ctx.DataStore.SelectDatabase(ctx.CurrentDB)
	value, exists := db.RPop(key)
	if !exists {
		resp.EncodeBulkString(conn, "")
		return
	}

	resp.EncodeBulkString(conn, value)
}

func handleLRange(command []string, conn io.Writer, ctx *datastore.ConnectionContext) {
	if len(command) < 4 {
		resp.EncodeError(conn, "ERR wrong number of arguments for 'LRANGE'")
		return
	}

	key := command[1]
	start, err1 := strconv.Atoi(command[2])
	stop, err2 := strconv.Atoi(command[3])
	if err1 != nil || err2 != nil {
		resp.EncodeError(conn, "ERR arguments for 'LRANGE' must be integers")
		return
	}

	db := ctx.DataStore.SelectDatabase(ctx.CurrentDB)
	values, exists := db.LRange(key, start, stop)
	if !exists {
		resp.EncodeArray(conn, nil)
		return
	}

	resp.EncodeArray(conn, values)
}
