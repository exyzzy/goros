// Package serial frames goros traffic over any byte stream (UART, USB CDC,
// TCP socket, pipe). This is the transport a TinyGo microcontroller uses to
// talk to a host, where `goros bridge` forwards frames onto UDP.
//
// Framing: each frame is COBS-encoded, followed by a CRC-16/CCITT of the raw
// frame (also inside the COBS block), and terminated by a 0x00 byte. COBS
// guarantees the encoded body contains no zero bytes, so receivers can
// resynchronise after corruption by scanning for the delimiter.
package serial

import (
	"errors"
	"io"
	"sync"
	"time"

	"bitbucket.org/goros/transport"
)

var ErrCRC = errors.New("serial: crc mismatch")

// Transport is a framed serial link.
type Transport struct {
	rw      io.ReadWriter
	wmu     sync.Mutex
	rbuf    []byte // pending raw bytes
	frame   []byte // encoded frame accumulator
	scratch []byte
	closed  bool
	mu      sync.Mutex
	maxLen  int
}

// New wraps rw. maxFrame bounds decoded frame size (0 = transport.MaxFrame).
func New(rw io.ReadWriter, maxFrame int) *Transport {
	if maxFrame <= 0 {
		maxFrame = transport.MaxFrame
	}
	return &Transport{rw: rw, rbuf: make([]byte, 256), frame: make([]byte, 0, 512), maxLen: maxFrame}
}

func (t *Transport) Name() string { return "serial" }

// CRC16 computes CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF).
func CRC16(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// cobsEncode appends the COBS encoding of src to dst.
func cobsEncode(dst, src []byte) []byte {
	codeIdx := len(dst)
	dst = append(dst, 0)
	code := byte(1)
	for _, b := range src {
		if b == 0 {
			dst[codeIdx] = code
			codeIdx = len(dst)
			dst = append(dst, 0)
			code = 1
			continue
		}
		dst = append(dst, b)
		code++
		if code == 0xFF {
			dst[codeIdx] = code
			codeIdx = len(dst)
			dst = append(dst, 0)
			code = 1
		}
	}
	dst[codeIdx] = code
	return dst
}

// cobsDecode decodes src (without the trailing delimiter) into dst.
func cobsDecode(dst, src []byte) ([]byte, bool) {
	i := 0
	for i < len(src) {
		code := int(src[i])
		if code == 0 || i+code > len(src)+1 {
			return dst, false
		}
		i++
		for j := 1; j < code; j++ {
			if i >= len(src) {
				return dst, false
			}
			dst = append(dst, src[i])
			i++
		}
		if code != 0xFF && i < len(src) {
			dst = append(dst, 0)
		}
	}
	return dst, true
}

func (t *Transport) Send(buf []byte) error {
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return transport.ErrClosed
	}
	crc := CRC16(buf)
	t.wmu.Lock()
	defer t.wmu.Unlock()
	t.scratch = t.scratch[:0]
	raw := append(append(t.scratch[:0], buf...), byte(crc>>8), byte(crc))
	enc := cobsEncode(make([]byte, 0, len(raw)+len(raw)/254+2), raw)
	enc = append(enc, 0)
	t.scratch = raw[:0]
	_, err := t.rw.Write(enc)
	return err
}

func (t *Transport) Recv() ([]byte, error) {
	for {
		n, err := t.rw.Read(t.rbuf)
		for _, b := range t.rbuf[:n] {
			if b != 0 {
				if len(t.frame) < t.maxLen+t.maxLen/254+8 {
					t.frame = append(t.frame, b)
				}
				continue
			}
			if len(t.frame) == 0 {
				continue
			}
			dec, ok := cobsDecode(make([]byte, 0, len(t.frame)), t.frame)
			t.frame = t.frame[:0]
			if !ok || len(dec) < 2 {
				continue // resync on next delimiter
			}
			body, sum := dec[:len(dec)-2], uint16(dec[len(dec)-2])<<8|uint16(dec[len(dec)-1])
			if CRC16(body) != sum {
				continue
			}
			return body, nil
		}
		if err != nil {
			t.mu.Lock()
			closed := t.closed
			t.mu.Unlock()
			if closed {
				return nil, transport.ErrClosed
			}
			return nil, err
		}
	}
}

func (t *Transport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	if c, ok := t.rw.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// ByteSerial is the subset of TinyGo's machine.Serialer / machine.UART
// interface the adapter needs.
type ByteSerial interface {
	Buffered() int
	ReadByte() (byte, error)
	Write(p []byte) (int, error)
}

// Adapt wraps a TinyGo serial port (machine.Serial, a *machine.UART, ...)
// as an io.ReadWriter whose Read blocks, yielding to the scheduler while
// no bytes are buffered:
//
//	link := serial.New(serial.Adapt(machine.Serial), 4096)
func Adapt(s ByteSerial) io.ReadWriter { return &byteSerialRW{s: s} }

type byteSerialRW struct {
	s ByteSerial
}

func (b *byteSerialRW) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for b.s.Buffered() == 0 {
		time.Sleep(500 * time.Microsecond)
	}
	n := 0
	for n < len(p) && b.s.Buffered() > 0 {
		c, err := b.s.ReadByte()
		if err != nil {
			break
		}
		p[n] = c
		n++
	}
	return n, nil
}

func (b *byteSerialRW) Write(p []byte) (int, error) { return b.s.Write(p) }
