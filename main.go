package main

import (
	"fmt"
	"gmr/go-cache/config"
	"gmr/go-cache/lib/logger"
	redisServer "gmr/go-cache/redis/server"
	"gmr/go-cache/tcp"
	"os"
)

var banner = `
                                   _           
                                  | |          
  ____  ___ _____ ____ _____  ____| |__  _____ 
 / _  |/ _ (_____) ___|____ |/ ___)  _ \| ___ |
( (_| | |_| |   ( (___/ ___ ( (___| | | | ____|
 \___ |\___/     \____)_____|\____)_| |_|_____)
(_____|                                                                        
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6389,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
}

func main() {
	fmt.Print(banner)
	logger.Info("go-cache start...")

	configFile := os.Getenv("GO_CACHE_CONFIG")
	if configFile == "" {
		if fileExist("redis.conf") {
			config.SetupConfig("redis.conf")
		} else {
			config.Properties = defaultProperties
		}
	} else {
		config.SetupConfig(configFile)
	}

	err := tcp.ListenAmdServeWithSignal(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, redisServer.MakeHandler())
	if err != nil {
		logger.Fatal(err)
	}
}

func fileExist(fileName string) bool {
	info, err := os.Stat(fileName)
	return err == nil && !info.IsDir()
}
