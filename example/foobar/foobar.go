package main

import (
	"context"
	"log"
	"strings"

	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/common/logger"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx"
)

func main() {
	logger.SetLoggerLevel(logger.LogLevelDebug)
	cli, err := rsocket.Connect().
		SetupPayload(payload.NewString("hello", "worle")).
		Transport("tcp://127.0.0.1:8088").
		Start()
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = cli.Close()
	}()
	cli.RequestResponse(payload.NewString(strings.Repeat("c", 4096), "foobar")).
		DoOnSuccess(func(ctx context.Context, s rx.Subscription, elem payload.Payload) {
			log.Println("data len:", len(elem.Data()))
			log.Println(elem)
		}).
		Subscribe(context.Background())
}
