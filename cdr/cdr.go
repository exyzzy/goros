// Package cdr implements the OMG Common Data Representation (CDR) encoding
// used by ROS 2 message serialization (XCDR1, as produced by rosidl/Fast-CDR).
//
// The encoder always produces little-endian CDR with the standard 4-byte
// RTPS encapsulation header (0x00 0x01 0x00 0x00). The decoder accepts both
// little- and big-endian encapsulations.
//
// The package is written to build cleanly under TinyGo: no reflection, no
// fmt, and allocation only when growing the output buffer.
package cdr

import (
	"errors"
	"math"
	"unicode/utf16"
)

// Encapsulation identifiers (OMG DDS-RTPS 10.2).
const (
	EncapsCDRBE   uint16 = 0x0000
	EncapsCDRLE   uint16 = 0x0001
	EncapsPLCDRBE uint16 = 0x0002
	EncapsPLCDRLE uint16 = 0x0003
)

// HeaderSize is the size of the serialized-data encapsulation header.
const HeaderSize = 4

var (
	ErrShort        = errors.New("cdr: unexpected end of data")
	ErrBadHeader    = errors.New("cdr: bad encapsulation header")
	ErrLength       = errors.New("cdr: invalid length")
	ErrBadString    = errors.New("cdr: string not NUL-terminated")
	ErrBoundsExceed = errors.New("cdr: sequence exceeds declared bound")
)

// Encoder serializes values into a CDR byte stream.
type Encoder struct {
	buf  []byte
	base int // offset at which alignment is measured from
}

// NewEncoder returns an encoder with the encapsulation header already written.
func NewEncoder() *Encoder {
	e := &Encoder{buf: make([]byte, 0, 64)}
	e.Reset()
	return e
}

// NewEncoderBuffer returns an encoder that appends to buf (its contents are
// discarded) and writes the encapsulation header.
func NewEncoderBuffer(buf []byte) *Encoder {
	e := &Encoder{buf: buf[:0]}
	e.Reset()
	return e
}

// Reset discards everything written so far and rewrites the header.
func (e *Encoder) Reset() {
	e.buf = e.buf[:0]
	e.buf = append(e.buf, 0x00, 0x01, 0x00, 0x00) // CDR_LE, options = 0
	e.base = HeaderSize
}

// Bytes returns the encoded data including the encapsulation header. The
// slice aliases the encoder's buffer and is valid until the next Reset.
func (e *Encoder) Bytes() []byte { return e.buf }

// Len returns the number of bytes written so far, header included.
func (e *Encoder) Len() int { return len(e.buf) }

// Align pads the stream so the next primitive of size n is naturally aligned
// relative to the start of the serialized data.
func (e *Encoder) Align(n int) {
	rem := (len(e.buf) - e.base) % n
	if rem == 0 {
		return
	}
	for pad := n - rem; pad > 0; pad-- {
		e.buf = append(e.buf, 0)
	}
}

func (e *Encoder) Bool(v bool) {
	if v {
		e.buf = append(e.buf, 1)
	} else {
		e.buf = append(e.buf, 0)
	}
}

func (e *Encoder) Uint8(v uint8) { e.buf = append(e.buf, v) }
func (e *Encoder) Int8(v int8)   { e.buf = append(e.buf, uint8(v)) }

func (e *Encoder) Uint16(v uint16) {
	e.Align(2)
	e.buf = append(e.buf, byte(v), byte(v>>8))
}
func (e *Encoder) Int16(v int16) { e.Uint16(uint16(v)) }

func (e *Encoder) Uint32(v uint32) {
	e.Align(4)
	e.buf = append(e.buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}
func (e *Encoder) Int32(v int32) { e.Uint32(uint32(v)) }

func (e *Encoder) Uint64(v uint64) {
	e.Align(8)
	e.buf = append(e.buf,
		byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}
func (e *Encoder) Int64(v int64)     { e.Uint64(uint64(v)) }
func (e *Encoder) Float32(v float32) { e.Uint32(math.Float32bits(v)) }
func (e *Encoder) Float64(v float64) { e.Uint64(math.Float64bits(v)) }

// String writes a CDR string: uint32 length including the NUL terminator,
// the bytes, then a NUL.
func (e *Encoder) String(s string) {
	e.Uint32(uint32(len(s) + 1))
	e.buf = append(e.buf, s...)
	e.buf = append(e.buf, 0)
}

// WString writes a wide string as a uint32 count of UTF-16 code units
// followed by the units (no terminator), matching Fast-CDR.
func (e *Encoder) WString(s string) {
	units := utf16.Encode([]rune(s))
	e.Uint32(uint32(len(units)))
	e.Align(2)
	for _, u := range units {
		e.buf = append(e.buf, byte(u), byte(u>>8))
	}
}

// SeqLen writes the length prefix of a sequence.
func (e *Encoder) SeqLen(n int) { e.Uint32(uint32(n)) }

// Bytes writes an octet sequence (length-prefixed).
func (e *Encoder) BytesSeq(b []byte) {
	e.SeqLen(len(b))
	e.buf = append(e.buf, b...)
}

// RawBytes appends bytes without a length prefix (for fixed-size arrays).
func (e *Encoder) RawBytes(b []byte) { e.buf = append(e.buf, b...) }

func (e *Encoder) BoolSeq(v []bool) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Bool(x)
	}
}
func (e *Encoder) Int8Seq(v []int8) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Int8(x)
	}
}
func (e *Encoder) Uint16Seq(v []uint16) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Uint16(x)
	}
}
func (e *Encoder) Int16Seq(v []int16) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Int16(x)
	}
}
func (e *Encoder) Uint32Seq(v []uint32) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Uint32(x)
	}
}
func (e *Encoder) Int32Seq(v []int32) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Int32(x)
	}
}
func (e *Encoder) Uint64Seq(v []uint64) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Uint64(x)
	}
}
func (e *Encoder) Int64Seq(v []int64) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Int64(x)
	}
}
func (e *Encoder) Float32Seq(v []float32) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Float32(x)
	}
}
func (e *Encoder) Float64Seq(v []float64) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.Float64(x)
	}
}
func (e *Encoder) StringSeq(v []string) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.String(x)
	}
}
func (e *Encoder) WStringSeq(v []string) {
	e.SeqLen(len(v))
	for _, x := range v {
		e.WString(x)
	}
}

// Decoder reads values from a CDR byte stream. Errors are sticky: the first
// failure is recorded, subsequent reads return zero values, and Err reports
// the failure. This keeps generated decoding code free of error plumbing.
type Decoder struct {
	buf  []byte
	pos  int
	base int
	le   bool
	err  error
}

// NewDecoder parses the encapsulation header of data and prepares to decode
// the payload that follows it.
func NewDecoder(data []byte) *Decoder {
	d := &Decoder{buf: data}
	d.Reset(data)
	return d
}

// Reset re-initializes the decoder over data.
func (d *Decoder) Reset(data []byte) {
	d.buf = data
	d.pos = 0
	d.err = nil
	if len(data) < HeaderSize {
		d.err = ErrBadHeader
		return
	}
	id := uint16(data[0])<<8 | uint16(data[1])
	switch id {
	case EncapsCDRLE, EncapsPLCDRLE:
		d.le = true
	case EncapsCDRBE, EncapsPLCDRBE:
		d.le = false
	default:
		d.err = ErrBadHeader
		return
	}
	d.pos = HeaderSize
	d.base = HeaderSize
}

// Err returns the first error encountered, if any.
func (d *Decoder) Err() error { return d.err }

// Remaining returns the number of unread bytes.
func (d *Decoder) Remaining() int { return len(d.buf) - d.pos }

// Pos returns the current read offset.
func (d *Decoder) Pos() int { return d.pos }

func (d *Decoder) fail(err error) {
	if d.err == nil {
		d.err = err
	}
}

func (d *Decoder) Align(n int) {
	rem := (d.pos - d.base) % n
	if rem != 0 {
		d.pos += n - rem
	}
}

func (d *Decoder) need(n int) bool {
	if d.err != nil {
		return false
	}
	if d.pos+n > len(d.buf) {
		d.fail(ErrShort)
		return false
	}
	return true
}

func (d *Decoder) Bool() bool {
	if !d.need(1) {
		return false
	}
	v := d.buf[d.pos] != 0
	d.pos++
	return v
}

func (d *Decoder) Uint8() uint8 {
	if !d.need(1) {
		return 0
	}
	v := d.buf[d.pos]
	d.pos++
	return v
}
func (d *Decoder) Int8() int8 { return int8(d.Uint8()) }

func (d *Decoder) Uint16() uint16 {
	d.Align(2)
	if !d.need(2) {
		return 0
	}
	b := d.buf[d.pos:]
	d.pos += 2
	if d.le {
		return uint16(b[0]) | uint16(b[1])<<8
	}
	return uint16(b[1]) | uint16(b[0])<<8
}
func (d *Decoder) Int16() int16 { return int16(d.Uint16()) }

func (d *Decoder) Uint32() uint32 {
	d.Align(4)
	if !d.need(4) {
		return 0
	}
	b := d.buf[d.pos:]
	d.pos += 4
	if d.le {
		return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	}
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}
func (d *Decoder) Int32() int32 { return int32(d.Uint32()) }

func (d *Decoder) Uint64() uint64 {
	d.Align(8)
	if !d.need(8) {
		return 0
	}
	b := d.buf[d.pos:]
	d.pos += 8
	if d.le {
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
			uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
	}
	return uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 |
		uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48 | uint64(b[0])<<56
}
func (d *Decoder) Int64() int64     { return int64(d.Uint64()) }
func (d *Decoder) Float32() float32 { return math.Float32frombits(d.Uint32()) }
func (d *Decoder) Float64() float64 { return math.Float64frombits(d.Uint64()) }

// SeqLen reads a sequence length prefix and sanity-checks it against the
// remaining data (each element needs at least elemSize bytes).
func (d *Decoder) SeqLen(elemSize int) int {
	n := d.Uint32()
	if d.err != nil {
		return 0
	}
	if elemSize < 1 {
		elemSize = 1
	}
	if int(n) > d.Remaining()/elemSize+1 && int(n) > d.Remaining() {
		d.fail(ErrLength)
		return 0
	}
	return int(n)
}

func (d *Decoder) String() string {
	n := d.Uint32()
	if d.err != nil {
		return ""
	}
	if n == 0 {
		return ""
	}
	if !d.need(int(n)) {
		return ""
	}
	b := d.buf[d.pos : d.pos+int(n)]
	d.pos += int(n)
	if b[n-1] != 0 {
		d.fail(ErrBadString)
		return ""
	}
	return string(b[:n-1])
}

func (d *Decoder) WString() string {
	n := d.Uint32()
	if d.err != nil {
		return ""
	}
	d.Align(2)
	if !d.need(int(n) * 2) {
		return ""
	}
	units := make([]uint16, n)
	for i := range units {
		b := d.buf[d.pos:]
		if d.le {
			units[i] = uint16(b[0]) | uint16(b[1])<<8
		} else {
			units[i] = uint16(b[1]) | uint16(b[0])<<8
		}
		d.pos += 2
	}
	return string(utf16.Decode(units))
}

// BytesSeq reads a length-prefixed octet sequence. The returned slice is a
// copy and does not alias the input.
func (d *Decoder) BytesSeq() []byte {
	n := d.SeqLen(1)
	if d.err != nil || !d.need(n) {
		return nil
	}
	out := make([]byte, n)
	copy(out, d.buf[d.pos:d.pos+n])
	d.pos += n
	return out
}

// RawBytes reads exactly n bytes without a length prefix into dst.
func (d *Decoder) RawBytes(dst []byte) {
	if !d.need(len(dst)) {
		return
	}
	copy(dst, d.buf[d.pos:d.pos+len(dst)])
	d.pos += len(dst)
}

func (d *Decoder) BoolSeq() []bool {
	n := d.SeqLen(1)
	if d.err != nil {
		return nil
	}
	v := make([]bool, n)
	for i := range v {
		v[i] = d.Bool()
	}
	return v
}
func (d *Decoder) Int8Seq() []int8 {
	n := d.SeqLen(1)
	if d.err != nil {
		return nil
	}
	v := make([]int8, n)
	for i := range v {
		v[i] = d.Int8()
	}
	return v
}
func (d *Decoder) Uint16Seq() []uint16 {
	n := d.SeqLen(2)
	if d.err != nil {
		return nil
	}
	v := make([]uint16, n)
	for i := range v {
		v[i] = d.Uint16()
	}
	return v
}
func (d *Decoder) Int16Seq() []int16 {
	n := d.SeqLen(2)
	if d.err != nil {
		return nil
	}
	v := make([]int16, n)
	for i := range v {
		v[i] = d.Int16()
	}
	return v
}
func (d *Decoder) Uint32Seq() []uint32 {
	n := d.SeqLen(4)
	if d.err != nil {
		return nil
	}
	v := make([]uint32, n)
	for i := range v {
		v[i] = d.Uint32()
	}
	return v
}
func (d *Decoder) Int32Seq() []int32 {
	n := d.SeqLen(4)
	if d.err != nil {
		return nil
	}
	v := make([]int32, n)
	for i := range v {
		v[i] = d.Int32()
	}
	return v
}
func (d *Decoder) Uint64Seq() []uint64 {
	n := d.SeqLen(8)
	if d.err != nil {
		return nil
	}
	v := make([]uint64, n)
	for i := range v {
		v[i] = d.Uint64()
	}
	return v
}
func (d *Decoder) Int64Seq() []int64 {
	n := d.SeqLen(8)
	if d.err != nil {
		return nil
	}
	v := make([]int64, n)
	for i := range v {
		v[i] = d.Int64()
	}
	return v
}
func (d *Decoder) Float32Seq() []float32 {
	n := d.SeqLen(4)
	if d.err != nil {
		return nil
	}
	v := make([]float32, n)
	for i := range v {
		v[i] = d.Float32()
	}
	return v
}
func (d *Decoder) Float64Seq() []float64 {
	n := d.SeqLen(8)
	if d.err != nil {
		return nil
	}
	v := make([]float64, n)
	for i := range v {
		v[i] = d.Float64()
	}
	return v
}
func (d *Decoder) StringSeq() []string {
	n := d.SeqLen(4)
	if d.err != nil {
		return nil
	}
	v := make([]string, n)
	for i := range v {
		v[i] = d.String()
	}
	return v
}
func (d *Decoder) WStringSeq() []string {
	n := d.SeqLen(4)
	if d.err != nil {
		return nil
	}
	v := make([]string, n)
	for i := range v {
		v[i] = d.WString()
	}
	return v
}
