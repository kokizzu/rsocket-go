package fragmentation

import (
	"container/list"

	"github.com/rsocket/rsocket-go/common"
	"github.com/rsocket/rsocket-go/framing"
	"github.com/rsocket/rsocket-go/payload"
)

const (
	// MinFragment is minimum fragment size in bytes.
	MinFragment = framing.HeaderLen + 4
	// MaxFragment is minimum fragment size in bytes.
	MaxFragment = common.MaxUint24
)

// HeaderAndPayload is Payload which having a FrameHeader.
type HeaderAndPayload interface {
	payload.Payload
	// Header returns a header of frame.
	Header() framing.FrameHeader
}

type Splitter interface {
	Split(keep int, data []byte, metadata []byte, onFrame func(idx int, fg framing.FrameFlag, body *common.ByteBuff)) error
	ShouldSplit(size int) bool
}

type Joiner interface {
	HeaderAndPayload
	First() framing.Frame
	Push(elem HeaderAndPayload) (end bool)
}

func NewJoiner(first HeaderAndPayload) Joiner {
	root := list.New()
	root.PushBack(first)
	return &implJoiner{
		root: root,
	}
}

func NewSplitter(fragment int) Splitter {
	return implSplitter(fragment)
}
