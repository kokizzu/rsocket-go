package fragmentation

import (
	"strings"
	"testing"

	"github.com/rsocket/rsocket-go/framing"
	"github.com/rsocket/rsocket-go/payload"
)

func BenchmarkToFragments(b *testing.B) {
	// 4m data + 1m metadata, 128k per fragment
	const n = 128
	data := strings.Repeat("d", 4*1024*1024)
	metadata := strings.Repeat("m", 1024*1024)
	origin := payload.NewString(data, metadata)
	h := framing.NewFrameHeader(77778888, framing.FrameTypePayload, framing.FlagMetadata, framing.FlagComplete)
	fn := func(frame framing.Frame) {
		frame.Release()
	}
	for i := 0; i < b.N; i++ {
		err := ToFragments(h, n, origin, fn)
		if err != nil {
			b.Error(err)
		}
	}
}
