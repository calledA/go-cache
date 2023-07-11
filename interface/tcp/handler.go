package tcp

import (
	"context"
	"net"
)

// application handler 方法
type HandleFunc func(ctx context.Context, conn net.Conn)

// tcp server的handler
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
