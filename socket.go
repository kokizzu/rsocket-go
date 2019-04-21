package rsocket

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/rsocket/rsocket-go/common"
	"github.com/rsocket/rsocket-go/common/logger"
	"github.com/rsocket/rsocket-go/fragmentation"
	"github.com/rsocket/rsocket-go/framing"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx"
	"github.com/rsocket/rsocket-go/transport"
)

var (
	errSocketClosed            = errors.New("socket closed already")
	unsupportedRequestStream   = []byte("Request-Stream not implemented.")
	unsupportedRequestResponse = []byte("Request-Response not implemented.")
	unsupportedRequestChannel  = []byte("Request-Channel not implemented.")
)

type duplexRSocket struct {
	responder RSocket
	tp        transport.Transport
	messages  *publishersMap
	scheduler rx.Scheduler
	sids      *genStreamID

	splitter  fragmentation.Splitter
	fragments *sync.Map // key=streamID, value=Joiner
}

func (p *duplexRSocket) Close() error {
	return p.tp.Close()
}

func (p *duplexRSocket) FireAndForget(sending payload.Payload) {
	defer sending.Release()
	data := sending.Data()
	size := framing.HeaderLen + len(sending.Data())
	m, ok := sending.Metadata()
	if ok {
		size += 3 + len(m)
	}
	sid := p.sids.next()
	if !p.splitter.ShouldSplit(size) {
		_ = p.tp.Send(framing.NewFrameFNF(sid, data, m))
		return
	}
	_ = p.splitter.Split(0, data, m, func(idx int, fg framing.FrameFlag, body *common.ByteBuff) {
		var f framing.Frame
		if idx == 0 {
			h := framing.NewFrameHeader(sid, framing.FrameTypeRequestFNF, fg)
			f = &framing.FrameFNF{
				BaseFrame: framing.NewBaseFrame(h, body),
			}
		} else {
			h := framing.NewFrameHeader(sid, framing.FrameTypeRequestResponse, fg|framing.FlagNext)
			f = &framing.FramePayload{
				BaseFrame: framing.NewBaseFrame(h, body),
			}
		}
		_ = p.tp.Send(f)
	})
}

func (p *duplexRSocket) MetadataPush(payload payload.Payload) {
	metadata, _ := payload.Metadata()
	_ = p.tp.Send(framing.NewFrameMetadataPush(metadata))
}

func (p *duplexRSocket) RequestResponse(pl payload.Payload) rx.Mono {
	sid := p.sids.next()
	resp := rx.NewMono(nil)
	resp.DoAfterSuccess(func(ctx context.Context, elem payload.Payload) {
		elem.Release()
	})

	p.messages.put(sid, &publishers{
		mode:      msgStoreModeRequestResponse,
		receiving: resp,
	})

	resp.
		DoFinally(func(ctx context.Context, sig rx.SignalType) {
			if sig == rx.SignalCancel {
				_ = p.tp.Send(framing.NewFrameCancel(sid))
			}
			p.messages.remove(sid)
		})

	data := pl.Data()
	metadata, _ := pl.Metadata()

	merge := struct {
		sid uint32
		d   []byte
		m   []byte
		r   rx.MonoProducer
		pl  payload.Payload
	}{sid, data, metadata, resp.(rx.MonoProducer), pl}

	p.scheduler.Do(context.Background(), func(ctx context.Context) {
		defer merge.pl.Release()
		size := calcPayloadFrameSize(data, metadata)
		if !p.splitter.ShouldSplit(size) {
			if err := p.tp.Send(framing.NewFrameRequestResponse(merge.sid, merge.d, merge.m)); err != nil {
				merge.r.Error(err)
			}
			return
		}
		_ = p.splitter.Split(0, merge.d, merge.m, func(idx int, fg framing.FrameFlag, body *common.ByteBuff) {
			var f framing.Frame
			if idx == 0 {
				h := framing.NewFrameHeader(merge.sid, framing.FrameTypeRequestResponse, fg)
				f = &framing.FrameRequestResponse{
					BaseFrame: framing.NewBaseFrame(h, body),
				}
			} else {
				h := framing.NewFrameHeader(merge.sid, framing.FrameTypePayload, fg|framing.FlagNext)
				f = &framing.FramePayload{
					BaseFrame: framing.NewBaseFrame(h, body),
				}
			}
			_ = p.tp.Send(f)
		})
	})
	return resp
}

func (p *duplexRSocket) RequestStream(sending payload.Payload) rx.Flux {
	sid := p.sids.next()
	flux := rx.NewFlux(nil)

	p.messages.put(sid, &publishers{
		mode:      msgStoreModeRequestStream,
		receiving: flux,
	})

	data := sending.Data()
	metadata, _ := sending.Metadata()
	merge := struct {
		sid uint32
		d   []byte
		m   []byte
		l   payload.Payload
	}{sid, data, metadata, sending}

	flux.
		DoAfterNext(func(ctx context.Context, elem payload.Payload) {
			elem.Release()
		}).
		DoFinally(func(ctx context.Context, sig rx.SignalType) {
			p.messages.remove(merge.sid)
			if sig == rx.SignalCancel {
				_ = p.tp.Send(framing.NewFrameCancel(merge.sid))
			}
		}).
		DoOnRequest(func(ctx context.Context, n int) {
			_ = p.tp.Send(framing.NewFrameRequestN(merge.sid, uint32(n)))
		}).
		DoOnSubscribe(func(ctx context.Context, s rx.Subscription) {
			defer merge.l.Release()
			initN := uint32(s.N())
			size := calcPayloadFrameSize(merge.d, merge.m) + 4
			if !p.splitter.ShouldSplit(size) {
				if err := p.tp.Send(framing.NewFrameRequestStream(merge.sid, initN, data, metadata)); err != nil {
					flux.(rx.Producer).Error(err)
				}
				return
			}
			_ = p.splitter.Split(4, data, metadata, func(idx int, fg framing.FrameFlag, body *common.ByteBuff) {
				var f framing.Frame
				if idx == 0 {
					h := framing.NewFrameHeader(merge.sid, framing.FrameTypeRequestStream, fg)
					// write init RequestN
					binary.BigEndian.PutUint32(body.Bytes(), initN)
					f = &framing.FrameRequestStream{
						BaseFrame: framing.NewBaseFrame(h, body),
					}
				} else {
					h := framing.NewFrameHeader(merge.sid, framing.FrameTypePayload, fg|framing.FlagNext)
					f = &framing.FramePayload{
						BaseFrame: framing.NewBaseFrame(h, body),
					}
				}
				_ = p.tp.Send(f)
			})
		})
	return flux
}

func (p *duplexRSocket) RequestChannel(publisher rx.Publisher) rx.Flux {
	sid := p.sids.next()
	sending := publisher.(rx.Flux)
	receiving := rx.NewFlux(nil)

	p.messages.put(sid, &publishers{
		mode:      msgStoreModeRequestChannel,
		sending:   sending,
		receiving: receiving,
	})

	receiving.DoFinally(func(ctx context.Context, sig rx.SignalType) {
		// TODO: graceful close
		p.messages.remove(sid)
	})

	var idx uint32
	merge := struct {
		pubs *publishersMap
		tp   transport.Transport
		sid  uint32
		i    *uint32
	}{p.messages, p.tp, sid, &idx}

	sending.
		DoFinally(func(ctx context.Context, sig rx.SignalType) {
			if sig == rx.SignalComplete {
				_ = merge.tp.Send(framing.NewFramePayload(merge.sid, nil, nil, framing.FlagComplete))
			}
		}).
		SubscribeOn(p.scheduler).
		Subscribe(context.Background(), rx.OnNext(func(ctx context.Context, sub rx.Subscription, item payload.Payload) {
			defer item.Release()
			// TODO: request N
			if atomic.AddUint32(merge.i, 1) == 1 {
				metadata, _ := item.Metadata()
				_ = merge.tp.Send(framing.NewFrameRequestChannel(merge.sid, math.MaxUint32, item.Data(), metadata, framing.FlagNext))
			} else {
				metadata, _ := item.Metadata()
				_ = merge.tp.Send(framing.NewFramePayload(merge.sid, item.Data(), metadata, framing.FlagNext))
			}
		}))

	return receiving
}

func (p *duplexRSocket) onFrameRequestResponse(frame framing.Frame) error {
	// fragment
	receiving, ok := p.doFragment(frame.(*framing.FrameRequestResponse))
	if !ok {
		return nil
	}
	return p.respondRequestResponse(receiving)
}

func (p *duplexRSocket) respondRequestResponse(receiving fragmentation.HeaderAndPayload) error {
	sid := receiving.Header().StreamID()
	// 1. execute socket handler
	sending, err := func() (mono rx.Mono, err error) {
		defer func() {
			err = toError(recover())
		}()
		mono = p.responder.RequestResponse(receiving)
		return
	}()
	// 2. sending error with panic
	if err != nil {
		_ = p.writeError(sid, err)
		return nil
	}
	// 3. sending error with unsupported handler
	if sending == nil {
		_ = p.writeError(sid, framing.NewFrameError(sid, common.ErrorCodeApplicationError, unsupportedRequestResponse))
		return nil
	}
	// 4. register publisher
	p.messages.put(sid, &publishers{
		mode:    msgStoreModeRequestResponse,
		sending: sending,
	})
	// 5. async subscribe publisher
	sending.
		DoFinally(func(ctx context.Context, sig rx.SignalType) {
			p.messages.remove(sid)
			receiving.Release()
		}).
		DoOnError(func(ctx context.Context, err error) {
			_ = p.writeError(sid, err)
		}).
		SubscribeOn(p.scheduler).
		Subscribe(context.Background(), rx.OnNext(func(ctx context.Context, sub rx.Subscription, item payload.Payload) {
			// TODO: support fragmentation
			v, ok := item.(*framing.FrameRequestResponse)
			if !ok || v != receiving {
				metadata, _ := item.Metadata()
				_ = p.tp.Send(framing.NewFramePayload(sid, item.Data(), metadata, framing.FlagNext|framing.FlagComplete))
				return
			}
			// reuse request frame, reduce copy
			fg := framing.FlagNext | framing.FlagComplete
			if _, ok := v.Metadata(); ok {
				fg |= framing.FlagMetadata
			}
			send := &framing.FramePayload{
				BaseFrame: framing.NewBaseFrame(framing.NewFrameHeader(sid, framing.FrameTypePayload, fg), v.Body()),
			}
			v.SetBody(nil)
			_ = p.tp.Send(send)
		}))
	return nil
}

func (p *duplexRSocket) onFrameRequestChannel(input framing.Frame) error {
	receiving, ok := p.doFragment(input.(*framing.FrameRequestChannel))
	if !ok {
		return nil
	}
	return p.respondRequestChannel(receiving)
}

func (p *duplexRSocket) respondRequestChannel(pl fragmentation.HeaderAndPayload) error {
	// seek initRequestN
	var initRequestN int
	switch v := pl.(type) {
	case *framing.FrameRequestChannel:
		initRequestN = int(v.InitialRequestN())
	case fragmentation.Joiner:
		initRequestN = int(v.First().(*framing.FrameRequestChannel).InitialRequestN())
	default:
		panic("unreachable")
	}

	sid := pl.Header().StreamID()
	receiving := rx.NewFlux(nil)
	sending, err := func() (flux rx.Flux, err error) {
		defer func() {
			err = toError(recover())
		}()
		flux = p.responder.RequestChannel(receiving.(rx.Flux))
		if flux == nil {
			err = framing.NewFrameError(sid, common.ErrorCodeApplicationError, unsupportedRequestChannel)
		}
		return
	}()
	if err != nil {
		return p.writeError(sid, err)
	}

	p.messages.put(sid, &publishers{
		mode:      msgStoreModeRequestChannel,
		sending:   sending,
		receiving: receiving,
	})

	if err := receiving.(rx.Producer).Next(pl); err != nil {
		pl.Release()
	}

	receiving.
		DoFinally(func(ctx context.Context, st rx.SignalType) {
			found, ok := p.messages.load(sid)
			if !ok {
				return
			}
			if found.sending == nil {
				p.messages.remove(sid)
			} else {
				found.receiving = nil
			}
		}).
		DoOnRequest(func(ctx context.Context, n int) {
			if n != math.MaxInt32 {
				_ = p.tp.Send(framing.NewFrameRequestN(sid, uint32(n)))
			}
		}).
		DoOnSubscribe(func(ctx context.Context, s rx.Subscription) {
			n := uint32(s.N())
			if n == math.MaxInt32 {
				_ = p.tp.Send(framing.NewFrameRequestN(sid, n))
			}
		})

	if receiving != sending {
		// auto release frame for each consumer
		receiving.DoAfterNext(func(ctx context.Context, item payload.Payload) {
			item.Release()
		})
	}

	sending.
		DoFinally(func(ctx context.Context, sig rx.SignalType) {
			found, ok := p.messages.load(sid)
			if !ok {
				return
			}
			if found.receiving == nil {
				p.messages.remove(sid)
			} else {
				found.sending = nil
			}
		}).
		DoOnError(func(ctx context.Context, err error) {
			_ = p.writeError(sid, err)
		}).
		DoOnComplete(func(ctx context.Context) {
			_ = p.tp.Send(framing.NewFramePayload(sid, nil, nil, framing.FlagComplete))
		}).
		SubscribeOn(p.scheduler).
		Subscribe(context.Background(), rx.OnSubscribe(func(ctx context.Context, s rx.Subscription) {
			s.Request(initRequestN)
		}), p.toSender(sid, framing.FlagNext))
	return nil
}

func (p *duplexRSocket) respondMetadataPush(input framing.Frame) error {
	p.scheduler.Do(context.Background(), func(ctx context.Context) {
		defer input.Release()
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("respond metadata push failed: %s\n", e)
			}
		}()
		p.responder.MetadataPush(input.(*framing.FrameMetadataPush))
	})
	return nil
}

func (p *duplexRSocket) onFrameFNF(frame framing.Frame) error {
	receiving, ok := p.doFragment(frame.(*framing.FrameFNF))
	if !ok {
		return nil
	}
	return p.respondFNF(receiving)
}

func (p *duplexRSocket) respondFNF(receiving fragmentation.HeaderAndPayload) (err error) {
	p.scheduler.Do(context.Background(), func(ctx context.Context) {
		defer receiving.Release()
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("respond FireAndForget failed: %s\n", e)
			}
		}()
		p.responder.FireAndForget(receiving)
	})
	return
}

func (p *duplexRSocket) onFrameRequestStream(frame framing.Frame) error {
	receiving, ok := p.doFragment(frame.(*framing.FrameRequestStream))
	if !ok {
		return nil
	}
	return p.respondRequestStream(receiving)
}

func (p *duplexRSocket) respondRequestStream(receiving fragmentation.HeaderAndPayload) error {
	sid := receiving.Header().StreamID()

	// 1. execute request stream handler
	resp, err := func() (resp rx.Flux, err error) {
		defer func() {
			err = toError(recover())
		}()
		resp = p.responder.RequestStream(receiving)
		if resp == nil {
			err = framing.NewFrameError(sid, common.ErrorCodeApplicationError, unsupportedRequestStream)
		}
		return
	}()

	// 2. send error with panic
	if err != nil {
		return p.writeError(sid, err)
	}

	// 3. register publisher
	p.messages.put(sid, &publishers{
		mode:    msgStoreModeRequestStream,
		sending: resp,
	})

	// 4. seek initRequestN
	var initRequestN int
	switch v := receiving.(type) {
	case *framing.FrameRequestStream:
		initRequestN = int(v.InitialRequestN())
	case fragmentation.Joiner:
		initRequestN = int(v.First().(*framing.FrameRequestStream).InitialRequestN())
	default:
		panic("unreachable")
	}

	// 5. async subscribe publisher
	resp.
		DoFinally(func(ctx context.Context, sig rx.SignalType) {
			p.messages.remove(sid)
			receiving.Release()
		}).
		DoOnComplete(func(ctx context.Context) {
			_ = p.tp.Send(framing.NewFramePayload(sid, nil, nil, framing.FlagComplete))
		}).
		DoOnError(func(ctx context.Context, err error) {
			_ = p.writeError(sid, err)
		}).
		SubscribeOn(p.scheduler).
		Subscribe(context.Background(), rx.OnSubscribe(func(ctx context.Context, s rx.Subscription) {
			s.Request(initRequestN)
		}), p.toSender(sid, framing.FlagNext))
	return nil
}

func (p *duplexRSocket) writeError(sid uint32, err error) error {
	v, ok := err.(*framing.FrameError)
	if ok {
		return p.tp.Send(v)
	}
	return p.tp.Send(framing.NewFrameError(sid, common.ErrorCodeApplicationError, []byte(err.Error())))
}

func (p *duplexRSocket) bindResponder(socket RSocket) {
	p.responder = socket
	p.tp.HandleRequestResponse(p.onFrameRequestResponse)
	p.tp.HandleMetadataPush(p.respondMetadataPush)
	p.tp.HandleFNF(p.onFrameFNF)
	p.tp.HandleRequestStream(p.onFrameRequestStream)
	p.tp.HandleRequestChannel(p.onFrameRequestChannel)
}

func (p *duplexRSocket) onFrameCancel(frame framing.Frame) error {
	defer frame.Release()
	sid := frame.Header().StreamID()
	if v, ok := p.messages.load(sid); ok {
		v.sending.(rx.Disposable).Dispose()
	}
	if joiner, ok := p.fragments.Load(sid); ok {
		joiner.(fragmentation.Joiner).Release()
		p.fragments.Delete(sid)
	}
	return nil
}

func (p *duplexRSocket) onFrameError(input framing.Frame) (err error) {
	f := input.(*framing.FrameError)
	logger.Errorf("handle error frame: %s\n", f)
	sid := f.Header().StreamID()
	v, ok := p.messages.load(sid)
	if !ok {
		return fmt.Errorf("invalid stream id: %d", sid)
	}
	switch v.mode {
	case msgStoreModeRequestResponse:
		v.receiving.(rx.Mono).DoFinally(func(ctx context.Context, st rx.SignalType) {
			f.Release()
		})
		v.receiving.(rx.MonoProducer).Error(f)
	case msgStoreModeRequestStream, msgStoreModeRequestChannel:
		v.receiving.(rx.Flux).DoFinally(func(ctx context.Context, st rx.SignalType) {
			f.Release()
		})
		v.receiving.(rx.Producer).Error(f)
	default:
		panic("unreachable")
	}
	return
}

func (p *duplexRSocket) onFrameRequestN(input framing.Frame) (err error) {
	defer input.Release()
	f := input.(*framing.FrameRequestN)
	sid := f.Header().StreamID()
	v, ok := p.messages.load(sid)
	if !ok {
		return fmt.Errorf("non-exists stream id: %d", sid)
	}
	// RequestN is always for sending.
	target := v.sending.(rx.Subscription)
	n := int(f.N())
	switch v.mode {
	case msgStoreModeRequestStream, msgStoreModeRequestChannel:
		target.Request(n)
	default:
		panic("unreachable")
	}
	return
}

func (p *duplexRSocket) doFragment(input fragmentation.HeaderAndPayload) (out fragmentation.HeaderAndPayload, ok bool) {
	h := input.Header()
	sid := h.StreamID()
	v, exist := p.fragments.Load(sid)
	if exist {
		joiner := v.(fragmentation.Joiner)
		ok = joiner.Push(input)
		if ok {
			p.fragments.Delete(sid)
			out = joiner
		}
		return
	}
	ok = !h.Flag().Check(framing.FlagFollow)
	if ok {
		out = input
		return
	}
	p.fragments.Store(sid, fragmentation.NewJoiner(input))
	return
}

func (p *duplexRSocket) onFramePayload(frame framing.Frame) error {
	pl, ok := p.doFragment(frame.(*framing.FramePayload))
	if !ok {
		return nil
	}
	h := pl.Header()
	t := h.Type()
	if t == framing.FrameTypeRequestFNF {
		return p.respondFNF(pl)
	}
	if t == framing.FrameTypeRequestResponse {
		return p.respondRequestResponse(pl)
	}
	if t == framing.FrameTypeRequestStream {
		return p.respondRequestStream(pl)
	}
	if t == framing.FrameTypeRequestChannel {
		return p.respondRequestChannel(pl)
	}

	sid := h.StreamID()
	v, ok := p.messages.load(sid)
	if !ok {
		return fmt.Errorf("non-exist stream id: %d", sid)
	}
	fg := h.Flag()
	switch v.mode {
	case msgStoreModeRequestResponse:
		if err := v.receiving.(rx.MonoProducer).Success(pl); err != nil {
			pl.Release()
			logger.Warnf("produce payload failed: %s\n", err.Error())
		}
	case msgStoreModeRequestStream, msgStoreModeRequestChannel:
		receiving := v.receiving.(rx.Producer)
		if fg.Check(framing.FlagNext) {
			if err := receiving.Next(pl); err != nil {
				pl.Release()
				logger.Warnf("produce payload failed: %s\n", err.Error())
			}
		}
		if fg.Check(framing.FlagComplete) {
			receiving.Complete()
			if !fg.Check(framing.FlagNext) {
				pl.Release()
			}
		}
	default:
		panic("unreachable")
	}
	return nil
}

func (p *duplexRSocket) start() {
	p.tp.HandleCancel(p.onFrameCancel)
	p.tp.HandleError(p.onFrameError)
	p.tp.HandleRequestN(p.onFrameRequestN)
	p.tp.HandlePayload(p.onFramePayload)
}

func (p *duplexRSocket) toSender(sid uint32, fg framing.FrameFlag) rx.OptSubscribe {
	merge := struct {
		tp  transport.Transport
		sid uint32
		fg  framing.FrameFlag
	}{p.tp, sid, fg}
	return rx.OnNext(func(ctx context.Context, sub rx.Subscription, elem payload.Payload) {
		switch v := elem.(type) {
		case *framing.FramePayload:
			if v.Header().Flag().Check(framing.FlagMetadata) {
				h := framing.NewFrameHeader(merge.sid, framing.FrameTypePayload, merge.fg|framing.FlagMetadata)
				v.SetHeader(h)
			} else {
				h := framing.NewFrameHeader(merge.sid, framing.FrameTypePayload, merge.fg)
				v.SetHeader(h)
			}
			_ = merge.tp.Send(v)
		default:
			defer elem.Release()
			metadata, _ := elem.Metadata()
			_ = merge.tp.Send(framing.NewFramePayload(merge.sid, elem.Data(), metadata, merge.fg))
		}
	})
}

func (p *duplexRSocket) send(frame framing.Frame) {
	// TODO: support fragment
}

func (p *duplexRSocket) releaseAll() {
	p.messages.m.Range(func(key, value interface{}) bool {
		vv := value.(*publishers)
		if vv.receiving != nil {
			vv.receiving.(errorProducer).Error(errSocketClosed)
		}
		if vv.sending != nil {
			vv.sending.(errorProducer).Error(errSocketClosed)
		}
		return true
	})
}

func newDuplexRSocket(tp transport.Transport, serverMode bool, scheduler rx.Scheduler, mtu int) *duplexRSocket {
	sk := &duplexRSocket{
		splitter:  fragmentation.NewSplitter(mtu),
		tp:        tp,
		messages:  newMessageStore(),
		scheduler: scheduler,
		sids: &genStreamID{
			serverMode: serverMode,
			cur:        0,
		},
		fragments: &sync.Map{},
	}
	tp.OnClose(sk.releaseAll)
	sk.start()
	return sk
}
