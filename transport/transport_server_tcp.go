package transport

import (
	"context"
	"net"
	"sync"

	"github.com/rsocket/rsocket-go/common"
	"github.com/rsocket/rsocket-go/common/logger"
	"github.com/rsocket/rsocket-go/framing"
)

type setupAcceptor = func(setup *framing.FrameSetup, tp Transport) error

type tcpServerTransport struct {
	addr      string
	acceptor  setupAcceptor
	listener  net.Listener
	onceClose *sync.Once
}

func (p *tcpServerTransport) Accept(acceptor setupAcceptor) {
	p.acceptor = acceptor
}

func (p *tcpServerTransport) Close() (err error) {
	if p.listener == nil {
		return
	}
	p.onceClose.Do(func() {
		err = p.listener.Close()
	})
	return
}

func (p *tcpServerTransport) Listen(onReady ...func()) (err error) {
	p.listener, err = net.Listen("tcp", p.addr)
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		if err == nil {
			err = p.Close()
		}
	}()

	go func() {
		for _, v := range onReady {
			v()
		}
	}()

	for {
		c, err := p.listener.Accept()
		if err != nil {
			logger.Errorf("protoTCP listener break: %s\n", err)
			return err
		}
		go func(ctx context.Context, c net.Conn) {
			select {
			case <-ctx.Done():
				_ = c.Close()
				return
			default:
				tp := newTransportClient(newTCPRConnection(c, common.DefaultKeepaliveInteval, common.DefaultKeepaliveMaxLifetime, false))
				tp.HandleSetup(func(f framing.Frame) (err error) {
					setup := f.(*framing.FrameSetup)
					defer setup.Release()
					if p.acceptor != nil {
						err = p.acceptor(setup, tp)
					}
					return
				})
				if err := tp.Start(ctx); err != nil {
					logger.Debugf("transport exit: %s\n", err.Error())
				}
			}
		}(ctx, c)
	}
}

// NewTCPServerTransport returns a new server-side transport on protoTCP networking.
func NewTCPServerTransport(addr string) (ServerTransport, error) {
	return &tcpServerTransport{
		addr:      addr,
		onceClose: &sync.Once{},
	}, nil
}
