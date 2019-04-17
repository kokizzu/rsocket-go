package rsocket

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/rsocket/rsocket-go/common"
	"github.com/rsocket/rsocket-go/common/logger"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx"
)

const (
	effort                = 5
	defaultMinActives     = 30
	defaultMaxActives     = 100
	defaultLowerQuantile  = 0.2
	defaultHigherQuantile = 0.8
	defaultExpFactor      = 4.0
)

var (
	errMustFailed          = errors.New("must failed")
	errAtLeastTwoSuppliers = errors.New("rsocket: at least two clients in balancer")
	mustFailed             = &mustFailedSocket{}
)

type balancer struct {
	bu                            *implClientBuilder
	mu                            *sync.Mutex
	actives                       []*weightedSocket
	suppliers                     *socketSupplierPool
	minActives, maxActives        int
	expFactor                     float64
	lowerQuantile, higherQuantile common.Quantile
}

func (p *balancer) FireAndForget(msg payload.Payload) {
	p.next().FireAndForget(msg)
}

func (p *balancer) MetadataPush(msg payload.Payload) {
	p.next().MetadataPush(msg)
}

func (p *balancer) RequestResponse(msg payload.Payload) rx.Mono {
	return p.next().RequestResponse(msg)
}

func (p *balancer) RequestStream(msg payload.Payload) rx.Flux {
	return p.next().RequestStream(msg)
}

func (p *balancer) RequestChannel(msgs rx.Publisher) rx.Flux {
	return p.next().RequestChannel(msgs)
}

func (p *balancer) Close() (err error) {
	var failed int
	for _, it := range p.actives {
		if err := it.Close(); err != nil {
			failed++
		}
	}
	if failed > 0 {
		err = fmt.Errorf("rsocket: close %d sockets failed", failed)
	}
	return
}

func (p *balancer) Rebalance(uris ...string) error {
	p.mu.Lock()
	defer func() {
		logger.Infof("===================== REBALANCE =======================\n")
		p.mu.Unlock()
	}()
	newest := make(map[string]*socketSupplier)
	for _, uri := range uris {
		newest[uri] = newSocketSupplier(p.bu, uri)
	}

	if len(newest) < 1 {
		return 

	}

	removes := make([]int, 0)
	for i, l := 0, len(p.actives); i < l; i++ {
		uri := p.actives[i].supplier.u
		if _, ok := newest[uri]; !ok {
			removes = append(removes, i)
		}
	}
	for _, idx := range removes {
		rm := p.actives[idx]
		p.actives[idx] = nil
		logger.Debugf("remove then close actived socket: %s\n", rm)
		_ = rm.Close()
	}
	survives := make([]*weightedSocket, 0)
	for i, l := 0, len(p.actives); i < l; i++ {
		it := p.actives[i]
		if it != nil {
			survives = append(survives, it)
		}
	}
	p.actives = survives
	p.suppliers.reset(newest)
}

func (p *balancer) next() ClientSocket {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refresh()
	n := len(p.actives)
	if n < 1 {
		return mustFailed
	}
	if n == 1 {
		first := p.actives[0]
		logger.Debugf("choose=%s \n", first)
		return first
	}

	// TODO: remove debug code
	// log for debug begin
	sb := strings.Builder{}
	for _, value := range p.actives {
		sb.WriteString(value.supplier.u)
		sb.WriteByte(';')
	}
	logger.Debugf("actives: %s\n", sb.String())
	// log for debug end

	var rsc1, rsc2 *weightedSocket
	for i := 0; i < effort; i++ {
		i1 := common.RandIntn(n)
		i2 := common.RandIntn(n - 1)
		if i2 >= i1 {
			i2++
		}
		rsc1 = p.actives[i1]
		rsc2 = p.actives[i2]
		if rsc1.availability > 0 && rsc2.availability > 0 {
			break
		}
		if i+1 == effort && p.suppliers.size() > 0 {
			p.acquire()
		}
	}
	w1 := p.algorithmicWeight(rsc1)
	w2 := p.algorithmicWeight(rsc2)

	if w1 < w2 {
		logger.Debugf("choose=%s, giveup=%s, %.8f > %.8f\n", rsc2, rsc1, w2, w1)
		return rsc2
	}
	logger.Debugf("choose=%s, giveup=%s, %.8f > %.8f\n", rsc1, rsc2, w1, w2)
	return rsc1
}

func (p *balancer) refresh() {
	n := len(p.actives)
	switch {
	case n > p.maxActives:
		// TODO: reduce active sockets
	case n < p.minActives:
		d := p.minActives - n
		for i := 0; i < d && p.suppliers.size() > 0; i++ {
			p.acquire()
		}
	}
}

func (p *balancer) acquire() bool {
	supplier, ok := p.suppliers.next()
	if !ok {
		logger.Debugf("rsocket: no socket supplier available\n")
		return false
	}
	logger.Debugf("choose supplier %s\n", supplier)
	sk, err := supplier.create(p.lowerQuantile, p.higherQuantile)
	if err != nil {
		_ = p.suppliers.returnSupplier(supplier)
		return false
	}
	p.actives = append(p.actives, sk)
	// TODO: ugly code
	merge := &struct {
		sk *weightedSocket
		ba *balancer
	}{sk, p}
	sk.origin.(*duplexRSocket).tp.OnClose(func() {
		merge.ba.mu.Lock()
		defer merge.ba.mu.Unlock()
		logger.Debugf("rsocket: unload %s\n", merge.sk)
		merge.ba.unload(merge.sk)
		_ = merge.ba.suppliers.returnSupplier(merge.sk.supplier)
	})
	return true
}

func (p *balancer) unload(socket *weightedSocket) {
	idx := -1
	for i := 0; i < len(p.actives); i++ {
		if p.actives[i] == socket {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}
	p.actives[idx] = p.actives[len(p.actives)-1]
	p.actives[len(p.actives)-1] = nil
	p.actives = p.actives[:len(p.actives)-1]
}

type mustFailedSocket struct {
}

func (p *mustFailedSocket) Close() error {
	return nil
}

func (*mustFailedSocket) FireAndForget(msg payload.Payload) {
	msg.Release()
}

func (*mustFailedSocket) MetadataPush(msg payload.Payload) {
	msg.Release()
}

func (*mustFailedSocket) RequestResponse(msg payload.Payload) rx.Mono {
	return rx.
		NewMono(func(ctx context.Context, sink rx.MonoProducer) {
			sink.Error(errMustFailed)
		}).
		DoFinally(func(ctx context.Context, st rx.SignalType) {
			msg.Release()
		})
}

func (*mustFailedSocket) RequestStream(msg payload.Payload) rx.Flux {
	return rx.
		NewFlux(func(ctx context.Context, producer rx.Producer) {
			producer.Error(errMustFailed)
		}).
		DoFinally(func(ctx context.Context, st rx.SignalType) {
			msg.Release()
		})
}

func (*mustFailedSocket) RequestChannel(msgs rx.Publisher) rx.Flux {
	return rx.
		NewFlux(func(ctx context.Context, producer rx.Producer) {
			producer.Error(errMustFailed)
		}).
		DoFinally(func(ctx context.Context, st rx.SignalType) {
			rx.ToFlux(msgs).
				DoOnSubscribe(func(ctx context.Context, s rx.Subscription) {
					s.Cancel()
				}).
				DoAfterNext(func(ctx context.Context, elem payload.Payload) {
					elem.Release()
				}).
				Subscribe(context.Background())
		})
}

func (p *balancer) algorithmicWeight(socket *weightedSocket) float64 {
	if socket.availability == 0 {
		return 0
	}
	pendings := float64(socket.pending)
	latency := socket.getPredictedLatency()
	//logger.Infof("%s: latency=%f\n", socket, latency)
	low := p.lowerQuantile.Estimation()
	high := math.Max(p.higherQuantile.Estimation(), low*1.001)
	bandWidth := math.Max(high-low, 1)
	if latency < low {
		alpha := (low - latency) / bandWidth
		bonusFactor := math.Pow(1+alpha, p.expFactor)
		latency /= bonusFactor
	} else if latency > high {
		alpha := (latency - high) / bandWidth
		penaltyFactor := math.Pow(1+alpha, p.expFactor)
		latency *= penaltyFactor
	}
	w := socket.availability / (1 + latency*(pendings+1))
	/*logger.Infof("%s: w=%f, high=%f, low=%f, availability=%f, pending=%f, latency=%f, \n",
	socket, w,
	high, low,
	socket.availability, pendings, latency)*/
	return w
}

func newBalancer(bu *implClientBuilder, pool *socketSupplierPool) *balancer {
	return &balancer{
		bu:             bu,
		mu:             &sync.Mutex{},
		actives:        make([]*weightedSocket, 0),
		suppliers:      pool,
		minActives:     defaultMinActives,
		maxActives:     defaultMaxActives,
		lowerQuantile:  common.NewFrugalQuantile(defaultLowerQuantile, 1),
		higherQuantile: common.NewFrugalQuantile(defaultHigherQuantile, 1),
		expFactor:      defaultExpFactor,
	}
}

func newBalancerStarter(bu *implClientBuilder, uris []string) *balancerStarter {
	return &balancerStarter{
		bu:   bu,
		uris: uris,
	}
}

type balancerStarter struct {
	bu   *implClientBuilder
	uris []string
}

func (p *balancerStarter) Start() (ClientSocket, error) {
	if len(p.uris) < 2 {
		return nil, errAtLeastTwoSuppliers
	}
	suppliers := make([]*socketSupplier, 0)
	for _, uri := range p.uris {
		suppliers = append(suppliers, newSocketSupplier(p.bu, uri))
	}
	pool := newSocketPool(suppliers...)
	return newBalancer(p.bu, pool), nil
}
