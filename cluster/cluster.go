package cluster

import (
	"fmt"
	"github.com/hdt3213/rdb/core"
	"goRedisPlus/config"
	database2 "goRedisPlus/database"
	"goRedisPlus/datastruct/dict"
	"goRedisPlus/datastruct/set"
	"goRedisPlus/interface/database"
	"goRedisPlus/interface/redis"
	"goRedisPlus/lib/idgenerator"
	"goRedisPlus/lib/logger"
	"goRedisPlus/redis/parser"
	"goRedisPlus/redis/protocol"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"
)

// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster

// Cluster represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type Cluster struct {
	self          string
	addr          string
	db            database.DBEngine // 每个单独的redis数据库上面都套了一个cluster，这个db就是那个单机版的redis
	transactions  *dict.SimpleDict  // id -> Transaction
	transactionMu sync.RWMutex
	topology      topology // 拓扑结构：使用raft
	slotMu        sync.RWMutex
	slots         map[uint32]*hostSlot // redis中的槽位
	idGenerator   *idgenerator.IDGenerator

	clientFactory clientFactory // 连接工厂
}

type peerClient interface {
	Send(args [][]byte) redis.Reply
}

type peerStream interface {
	Stream() <-chan *parser.Payload
	Close() error
}

type clientFactory interface {
	GetPeerClient(peerAddr string) (peerClient, error)
	ReturnPeerClient(peerAddr string, peerClient peerClient) error
	NewStream(peerAddr string, cmdLine CmdLine) (peerStream, error)
	Close() error
}

const (
	slotStateHost = iota
	slotStateImporting
	slotStateMovingOut
)

// hostSlot stores status of host which hosted by current node
//这个结构体是用于表示 Redis 集群中的槽位（slot）的状态和相关信息的。
//在 Redis 集群中，数据被分散存储在多个节点上，每个节点负责管理一部分槽位。
//槽位是 Redis 集群中数据分片的基本单位，总共有 16384 个槽位。
type hostSlot struct {
	state uint32
	mu    sync.RWMutex
	// OldNodeID is the node which is moving out this slot
	// only valid during slot is importing
	oldNodeID string
	// OldNodeID is the node which is importing this slot
	// only valid during slot is moving out
	newNodeID string

	/* importedKeys stores imported keys during migrating progress
	 * While this slot is migrating, if importedKeys does not have the given key, then current node will import key before execute commands
	 *
	 * In a migrating slot, the slot on the old node is immutable, we only delete a key in the new node.
	 * Therefore, we must distinguish between non-migrated key and deleted key.
	 * Even if a key has been deleted, it still exists in importedKeys, so we can distinguish between non-migrated and deleted.
	 */
	importedKeys *set.Set
	// keys stores all keys in this slot
	// Cluster.makeInsertCallback and Cluster.makeDeleteCallback will keep keys up to time
	keys *set.Set
}

// if only one node involved in a transaction, just execute the command don't apply tcc procedure
var allowFastTransaction = true

// MakeCluster creates and starts a node of cluster
func MakeCluster() *Cluster {
	cluster := &Cluster{
		self:          config.Properties.Self,
		addr:          config.Properties.AnnounceAddress(),
		db:            database2.NewStandaloneServer(), // 底层单机redis
		transactions:  dict.MakeSimple(),
		idGenerator:   idgenerator.MakeGenerator(config.Properties.Self),
		clientFactory: newDefaultClientFactory(), // 默认连接池
	}
	topologyPersistFile := path.Join(config.Properties.Dir, config.Properties.ClusterConfigFile) // 拓扑持久化文件
	cluster.topology = newRaft(cluster, topologyPersistFile)
	cluster.db.SetKeyInsertedCallback(cluster.makeInsertCallback()) // 每次插入key之后都要把key插入到对应的slot的set中
	cluster.db.SetKeyDeletedCallback(cluster.makeDeleteCallback())  // 每次删除key之后都要把key从对应的slot的set中删除
	cluster.slots = make(map[uint32]*hostSlot)
	var err error
	if topologyPersistFile != "" && fileExists(topologyPersistFile) {
		err = cluster.LoadConfig()
	} else if config.Properties.ClusterAsSeed { // 作为初始节点启动
		err = cluster.startAsSeed(config.Properties.AnnounceAddress())
	} else {
		err = cluster.Join(config.Properties.ClusterSeed)
	}
	if err != nil {
		panic(err)
	}
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

// Close stops current node of cluster
func (cluster *Cluster) Close() {
	_ = cluster.topology.Close()
	cluster.db.Close()
	cluster.clientFactory.Close()
}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// Exec executes command on cluster
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "info" {
		if ser, ok := cluster.db.(*database2.Server); ok {
			return database2.Info(ser, cmdLine[1:])
		}
	}
	if cmdName == "auth" {
		return database2.Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}

	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return database2.DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(cluster, c, nil)
	} else if cmdName == "select" {
		return protocol.MakeErrReply("select not supported in cluster")
	}
	if c != nil && c.InMultiState() {
		return database2.EnqueueCmd(c, cmdLine)
	}
	cmdFunc, ok := router[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose does some clean after client close connection
func (cluster *Cluster) AfterClientClose(c redis.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *Cluster) LoadRDB(dec *core.Decoder) error {
	return cluster.db.LoadRDB(dec)
}

func (cluster *Cluster) makeInsertCallback() database.KeyEventCallback {
	return func(dbIndex int, key string, entity *database.DataEntity) {
		slotId := getSlot(key) // 获取key的hash值，也是用的crc32.然后取余16384，得到key所在的槽位id
		cluster.slotMu.RLock()
		slot, ok := cluster.slots[slotId] // 获取这个槽位
		cluster.slotMu.RUnlock()
		// As long as the command is executed, we should update slot.keys regardless of slot.state
		if ok {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			slot.keys.Add(key) // 向这个槽位中添加这个key
		}
	}
}

func (cluster *Cluster) makeDeleteCallback() database.KeyEventCallback {
	return func(dbIndex int, key string, entity *database.DataEntity) {
		slotId := getSlot(key)
		cluster.slotMu.RLock()
		slot, ok := cluster.slots[slotId]
		cluster.slotMu.RUnlock()
		// As long as the command is executed, we should update slot.keys regardless of slot.state
		if ok {
			slot.mu.Lock()
			defer slot.mu.Unlock()
			slot.keys.Remove(key)
		}
	}
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
