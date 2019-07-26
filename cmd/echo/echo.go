package main

import (
	"context"
	"fmt"
	"log"
	//_ "net/http/pprof"
	"strconv"
	"time"

	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/rsocket/rsocket-go"
	"github.com/rsocket/rsocket-go/internal/common"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx"
	"github.com/rsocket/rsocket-go/rx/flux"
	"github.com/rsocket/rsocket-go/rx/mono"
)

const ListenAt = "tcp://127.0.0.1:7878"

//const ListenAt = "unix:///tmp/rsocket.echo.sock"
//const ListenAt = "ws://127.0.0.1:7878/echo"

func main() {
	//go func() {
	//	log.Println(http.ListenAndServe(":4444", nil))
	//}()
	//logger.SetLevel(logger.LevelDebug)
	//go common.TraceByteBuffLeak(context.Background(), 10*time.Second)
	err := rsocket.Receive().
		//Fragment(65535).
		//Resume().
		Acceptor(func(setup payload.SetupPayload, sendingSocket rsocket.CloseableRSocket) rsocket.RSocket {
			//log.Println("SETUP BEGIN:----------------")
			//log.Println("maxLifeTime:", setup.MaxLifetime())
			//log.Println("keepaliveInterval:", setup.TimeBetweenKeepalive())
			//log.Println("dataMimeType:", setup.DataMimeType())
			//log.Println("metadataMimeType:", setup.MetadataMimeType())
			//log.Println("data:", string(setup.Data()))
			//log.Println("metadata:", string(setup.Metadata()))
			//log.Println("SETUP END:----------------")

			// NOTICE: request client for something.
			//sendingSocket.
			//	RequestResponse(payload.NewString("ping", "From server")).
			//	DoOnSuccess(func(ctx context.Context, s rx.Subscription, elem payload.Payload) {
			//		log.Println("rcv response from client:", elem)
			//	}).
			//	SubscribeOn(rx.ElasticScheduler()).
			//	Subscribe(context.Background())

			sendingSocket.OnClose(func() {
				log.Println("***** socket disconnected *****")
				time.AfterFunc(5*time.Second, func() {
					log.Println("leaks:", common.CountByteBuffer())
				})
			})

			return responder()
		}).
		Transport(ListenAt).
		Serve(context.Background())
	if err != nil {
		panic(err)
	}
}

func responder() rsocket.RSocket {
	return rsocket.NewAbstractSocket(
		rsocket.MetadataPush(func(item payload.Payload) {
			log.Println("GOT METADATA_PUSH:", item)
		}),
		rsocket.FireAndForget(func(elem payload.Payload) {
			log.Println("GOT FNF:", elem)
		}),
		rsocket.RequestResponse(func(pl payload.Payload) mono.Mono {
			// just echo
			return mono.Just(pl)

			// Graceful with context API.
			//return rx.NewMono(func(ctx context.Context, sink rx.MonoProducer) {
			//	time.Sleep(50 * time.Millisecond)
			//	select {
			//	case <-ctx.Done():
			//		break
			//	default:
			//		sink.Success(payload.Clone(pl))
			//	}
			//})
		}),
		rsocket.RequestStream(func(pl payload.Payload) flux.Flux {
			// for test: client metadata is totals as string

			// Here is my Java client code:
			//@Test
			//public void testRequestStream() {
			//	final int totals = 20;
			//	final CountDownLatch cdl = new CountDownLatch(totals);
			//	this.client.requestStream(
			//		DefaultPayload.create(RandomStringUtils.randomAlphanumeric(32), String.valueOf(totals)))
			//	.doOnNext(payload -> {
			//		log.info("income: data={}, metadata={}", payload.getDataUtf8(), payload.getMetadataUtf8());
			//		cdl.countDown();
			//	})
			//	.subscribe();
			//	try {
			//		cdl.await();
			//	} catch (InterruptedException e) {
			//		Thread.currentThread().interrupt();
			//		throw new Errorf(e);
			//	}
			//}
			s := pl.DataUTF8()
			m, _ := pl.MetadataUTF8()
			log.Println("data:", s, "metadata:", m)
			totals := 10
			if n, err := strconv.Atoi(m); err == nil {
				totals = n
			}
			return flux.Create(func(ctx context.Context, emitter flux.Sink) {
				for i := 0; i < totals; i++ {
					// You can use context for graceful coroutine shutdown, stop produce.
					select {
					case <-ctx.Done():
						log.Println("ctx done:", ctx.Err())
						return
					default:
						//time.Sleep(10 * time.Millisecond)
						emitter.Next(payload.NewString(fmt.Sprintf("%s_%d", s, i), m))
					}
				}
				emitter.Complete()
			})
		}),
		rsocket.RequestChannel(func(payloads rx.Publisher) flux.Flux {
			//return payloads.(flux.Flux)
			payloads.(flux.Flux).
				//LimitRate(1).
				SubscribeOn(scheduler.Elastic()).
				DoOnNext(func(elem payload.Payload) {
					log.Println("receiving:", elem)
				}).
				Subscribe(context.Background())
			return flux.Create(func(i context.Context, sink flux.Sink) {
				for i := 0; i < 3; i++ {
					sink.Next(payload.NewString("world", fmt.Sprintf("%d", i)))
				}
				sink.Complete()
			})

			//return payloads.(rx.Flux)
			// echo all incoming payloads
			//return rx.NewFlux(func(ctx context.Context, emitter rx.Producer) {
			//	payloads.(rx.Flux).
			//		DoFinally(func(ctx context.Context, st rx.SignalType) {
			//			emitter.Complete()
			//		}).
			//		DoOnNext(func(ctx context.Context, s rx.Subscription, elem payload.Payload) {
			//			metadata, _ := elem.Metadata()
			//			_ = emitter.Next(payload.New(elem.Data(), metadata))
			//		}).
			//		SubscribeOn(rx.ElasticScheduler()).
			//		Subscribe(context.Background())
			//})
		}),
	)
}
