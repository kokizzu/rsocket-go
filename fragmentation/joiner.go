package fragmentation

import (
	"container/list"
	"errors"
	"fmt"

	"github.com/rsocket/rsocket-go/framing"
)

var errNoFrameInJoiner = errors.New("no frames in current joiner")

type implJoiner struct {
	root *list.List // list of HeaderAndPayload
}

func (p *implJoiner) First() framing.Frame {
	first := p.root.Front()
	if first == nil {
		panic(errNoFrameInJoiner)
	}
	return first.Value.(framing.Frame)
}

func (p *implJoiner) Header() framing.FrameHeader {
	return p.First().Header()
}

func (p *implJoiner) String() string {
	m, _ := p.MetadataUTF8()
	return fmt.Sprintf("implJoiner{data=%s,metadata=%s}", p.DataUTF8(), m)
}

func (p *implJoiner) Metadata() (metadata []byte, ok bool) {
	for cur := p.root.Front(); cur != nil; cur = cur.Next() {
		f := cur.Value.(HeaderAndPayload)
		if !f.Header().Flag().Check(framing.FlagMetadata) {
			break
		}
		if m, has := f.Metadata(); has {
			metadata = append(metadata, m...)
			ok = true
		}
	}
	return
}

func (p *implJoiner) MetadataUTF8() (metadata string, ok bool) {
	var m []byte
	m, ok = p.Metadata()
	if ok {
		metadata = string(m)
	}
	return
}

func (p *implJoiner) Data() (data []byte) {
	for cur := p.root.Front(); cur != nil; cur = cur.Next() {
		f := cur.Value.(HeaderAndPayload)
		if d := f.Data(); len(d) > 0 {
			data = append(data, d...)
		}
	}
	return
}

func (p *implJoiner) DataUTF8() (data string) {
	if d := p.Data(); len(d) > 0 {
		data = string(d)
	}
	return
}

func (p *implJoiner) Release() {
	for {
		cur := p.root.Front()
		if cur == nil {
			break
		}
		v := p.root.Remove(cur).(HeaderAndPayload)
		v.Release()
	}
}

func (p *implJoiner) Push(elem HeaderAndPayload) (end bool) {
	p.root.PushBack(elem)
	h := elem.Header()
	end = !h.Flag().Check(framing.FlagFollow)
	return
}
