package fragmentation

import (
	"fmt"

	"github.com/rsocket/rsocket-go/common"
	"github.com/rsocket/rsocket-go/framing"
	"github.com/rsocket/rsocket-go/payload"
)

var errInvalidFragmentLen = fmt.Errorf("invalid fragment: [%d,%d]", minFragment, maxFragment)

type implSplitter int

func (s implSplitter) Split(h framing.FrameHeader, origin payload.Payload, onFrame func(frame framing.Frame)) (err error) {
	fragment := int(s)
	if fragment < minFragment || fragment > maxFragment {
		err = errInvalidFragmentLen
		return
	}
	metadata, _ := origin.Metadata()
	data := origin.Data()
	var bf *common.ByteBuff
	lenM, lenD := len(metadata), len(data)
	var cur1, cur2 int
	for {
		var wrote int
		bf = common.BorrowByteBuffer()
		var wroteM int
		left := fragment - framing.HeaderLen
		hasMetadata := cur1 < lenM
		if hasMetadata {
			left -= 3
			// write metadata length placeholder
			_ = bf.WriteUint24(0)
		}
		for wrote = 0; wrote < left; wrote++ {
			if cur1 < lenM {
				_ = bf.WriteByte(metadata[cur1])
				wroteM++
				cur1++
			} else if cur2 < lenD {
				_ = bf.WriteByte(data[cur2])
				cur2++
			}
		}
		follow := cur1+cur2 < lenM+lenD
		fg := h.Flag()
		if follow {
			fg |= framing.FlagFollow
		} else {
			fg &= ^framing.FlagFollow
		}
		if wroteM > 0 {
			// set metadata length
			x := common.NewUint24(wroteM)
			for i := 0; i < len(x); i++ {
				bf.B[i] = x[i]
			}
			fg |= framing.FlagMetadata
		} else {
			// non-metadata
			fg &= ^framing.FlagMetadata
		}
		h2 := framing.NewFrameHeader(h.StreamID(), h.Type(), fg)

		var frame framing.Frame
		frame, err = autoCreateFrame(h2, bf)
		if err != nil {
			return
		}
		if onFrame != nil {
			onFrame(frame)
		}
		if !follow {
			break
		}
	}
	return
}

func autoCreateFrame(h framing.FrameHeader, body *common.ByteBuff) (frame framing.Frame, err error) {
	base := framing.NewBaseFrame(h, body)
	switch h.Type() {
	case framing.FrameTypeMetadataPush:
		frame = &framing.FrameMetadataPush{BaseFrame: base}
	case framing.FrameTypeRequestFNF:
		frame = &framing.FrameFNF{BaseFrame: base}
	case framing.FrameTypePayload:
		frame = &framing.FramePayload{BaseFrame: base}
	case framing.FrameTypeRequestResponse:
		frame = &framing.FrameRequestResponse{BaseFrame: base}
	case framing.FrameTypeRequestStream:
		frame = &framing.FrameRequestStream{BaseFrame: base}
	case framing.FrameTypeRequestChannel:
		frame = &framing.FrameRequestChannel{BaseFrame: base}
	default:
		err = fmt.Errorf("illegal frame type %s", h.Type())
	}
	return
}
