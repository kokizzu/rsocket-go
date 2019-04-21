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
		Fragment(128).
		SetupPayload(payload.NewString("hello", "world")).
		Transport("tcp://127.0.0.1:8000").
		Start()
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = cli.Close()
	}()
	sending := payload.NewString(strings.Repeat("g", 373), "foobar")
	/*cli.RequestResponse(sending).
	DoOnSuccess(func(ctx context.Context, s rx.Subscription, elem payload.Payload) {
		log.Println("data len:", len(elem.Data()))
		log.Println(elem)
	}).
	Subscribe(context.Background())*/

	cli.RequestStream(sending).
		DoOnNext(func(ctx context.Context, s rx.Subscription, elem payload.Payload) {
			log.Println("data len:", len(elem.Data()))
			log.Println(elem)
		}).
		Subscribe(context.Background())
}
