package main

import (
	"fmt"
	"goRedisPlus/config"
	"goRedisPlus/lib/logger"
	"goRedisPlus/lib/utils"
	RedisServer "goRedisPlus/redis/server"
	"goRedisPlus/tcp"
	"os"
)

var banner = `goRedis prepare to start`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6399,
	AppendOnly:     true,
	AppendFilename: "appendonly.aof",
	MaxClients:     1000,
	RunID:          utils.RandString(40),
}

const configFile string = "redis.conf"

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

func main() {
	fmt.Println(banner)
	// 设置日志的输出位置和名字
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "goRedis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})
	// 设置配置文件
	if fileExists(configFile) {
		config.SetupConfig(configFile)
	} else {
		config.Properties = defaultProperties
	}
	// 开启监听
	err := tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler())
	if err != nil {
		logger.Error(err)
	}
}
