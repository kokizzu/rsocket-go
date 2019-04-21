package common

import (
	"fmt"
	"io"
)

// MaxUint24 is the max value of Uint24.
const MaxUint24 = 16777215

var errMaxUint24 = fmt.Errorf("uint24 exceed max value: %d", MaxUint24)

// Uint24 is 3 bytes unsigned integer.
type Uint24 [3]byte

// Bytes returns bytes encoded.
func (p Uint24) Bytes() []byte {
	return p[:]
}

// WriteTo encode and write bytes to a writer.
func (p Uint24) WriteTo(w io.Writer) (int64, error) {
	wrote, err := w.Write(p[:])
	return int64(wrote), err
}

// AsInt converts to int.
func (p Uint24) AsInt() int {
	return int(p[0])<<16 + int(p[1])<<8 + int(p[2])
}

// NewUint24 returns a new uint24.
func NewUint24(n int) (v Uint24) {
	if n > MaxUint24 {
		panic(errMaxUint24)
	}
	v[0] = byte(n >> 16)
	v[1] = byte(n >> 8)
	v[2] = byte(n)
	return
}

// NewUint24Bytes returns a new uint24 from bytes.
func NewUint24Bytes(bs []byte) Uint24 {
	_ = bs[2]
	return [3]byte{bs[0], bs[1], bs[2]}
}
