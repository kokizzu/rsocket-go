package fragmentation

import (
	"container/list"

	"github.com/rsocket/rsocket-go/common"
	"github.com/rsocket/rsocket-go/framing"
	"github.com/rsocket/rsocket-go/payload"
)

const (
	minFragment = framing.HeaderLen + 4
	maxFragment = common.MaxUint24
)

type HeaderAndPayload interface {
	payload.Payload
	Header() framing.FrameHeader
}

type Splitter interface {
	Split(h framing.FrameHeader, origin payload.Payload, onFrame func(frame framing.Frame)) error
}

type Joiner interface {
	HeaderAndPayload
	Push(elem HeaderAndPayload) (end bool)
}

func NewJoiner(first HeaderAndPayload) Joiner {
	root := list.New()
	root.PushBack(first)
	return &implJoiner{
		header: first.Header(),
		root:   root,
	}
}

func NewSplitter(fragment int) Splitter {
	return implSplitter(fragment)
}
