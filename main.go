package main

import (
	"github.com/liweiyuan/go-redis-server/command"
	"github.com/liweiyuan/go-redis-server/network"
	"github.com/liweiyuan/go-redis-server/storage"
)

func main() {
	s := storage.NewStorage()
	cr := command.NewCommandRegistry()
	network.Start(s, cr)
}
