package transport

import (
	"container/list"
	"fmt"

	"github.com/rsocket/rsocket-go/framing"
)

type FragmentPayload struct {
	sid  uint32
	fg   framing.FrameFlag
	root *list.List
}

func (p *FragmentPayload) Flag() framing.FrameFlag {
	return p.fg
}

func (p *FragmentPayload) StreamID() uint32 {
	return p.sid
}

func (p *FragmentPayload) String() string {
	m, _ := p.MetadataUTF8()
	return fmt.Sprintf("FragmentPayload{data=%s,metadata=%s}", p.DataUTF8(), m)
}

func (p *FragmentPayload) Metadata() (metadata []byte, ok bool) {
	for cur := p.root.Front(); cur != nil; cur = cur.Next() {
		f := cur.Value.(*framing.FramePayload)
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

func (p *FragmentPayload) MetadataUTF8() (metadata string, ok bool) {
	var m []byte
	m, ok = p.Metadata()
	if ok {
		metadata = string(m)
	}
	return
}

func (p *FragmentPayload) Data() (data []byte) {
	for cur := p.root.Front(); cur != nil; cur = cur.Next() {
		f := cur.Value.(*framing.FramePayload)
		if d := f.Data(); len(d) > 0 {
			data = append(data, d...)
		}
	}
	return
}

func (p *FragmentPayload) DataUTF8() (data string) {
	if d := p.Data(); len(d) > 0 {
		data = string(d)
	}
	return
}

func (p *FragmentPayload) Release() {
	for {
		cur := p.root.Front()
		if cur == nil {
			break
		}
		v := p.root.Remove(cur).(*framing.FramePayload)
		v.Release()
	}
}

func (p *FragmentPayload) push(frame *framing.FramePayload) (finish bool) {
	p.root.PushBack(frame)
	h := frame.Header()
	finish = !h.Flag().Check(framing.FlagFollow)
	return
}

func newFragmentPayload(first *framing.FramePayload) *FragmentPayload {
	root := list.New()
	root.PushBack(first)
	return &FragmentPayload{
		sid:  first.Header().StreamID(),
		fg:   first.Header().Flag(),
		root: root,
	}
}
