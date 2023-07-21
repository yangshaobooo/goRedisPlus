package server

import (
	"context"
	database2 "goRedisPlus/database"
	"goRedisPlus/interface/database"
	"goRedisPlus/lib/logger"
	"goRedisPlus/lib/sync/atomic"
	"goRedisPlus/redis/connection"
	"goRedisPlus/redis/parser"
	"goRedisPlus/redis/protocol"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.DB
	closing    atomic.Boolean // refusing new client and new request
}

// MakeHandler creates a Handler instance
func MakeHandler() *Handler {
	var db database.DB
	// 先不考虑集群
	//if config.Properties.ClusterEnable {
	//	// 创建集群数据库
	//	db = cluster.MakeCluster()
	//} else {
	//	// 创建常规的数据库
	//	db = database2.NewStandaloneServer()
	//}
	db = database2.NewStandaloneServer()
	return &Handler{
		db: db,
	}
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle receives and executes redis commands
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)     // 创建一个连接
	h.activeConn.Store(client, struct{}{}) // 把这个连接存起来

	ch := parser.ParseStream(conn) // 解析协议收到的数据，数据放到ch中
	for payload := range ch {      // 遍历每一个接受的payload
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply) //接收到的数据类型断言
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args) //执行接收到的命令
		if result != nil {
			_, _ = client.Write(result.ToBytes()) // 把执行的回复写回conn
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
