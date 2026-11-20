package serial

import (
	"bytes"
	"io"
	"testing"
)

func TestCOBS(t *testing.T) {
	cases := [][]byte{{}, {0}, {1, 2, 3}, {0, 0, 1, 0}, bytes.Repeat([]byte{7}, 300), append(bytes.Repeat([]byte{1}, 254), 0, 5)}
	for _, c := range cases {
		enc := cobsEncode(nil, c)
		if bytes.IndexByte(enc, 0) >= 0 {
			t.Fatalf("zero in encoding of % x", c)
		}
		dec, ok := cobsDecode(nil, enc)
		if !ok || !bytes.Equal(dec, c) {
			t.Fatalf("roundtrip % x -> % x -> % x", c, enc, dec)
		}
	}
}

func TestCRC(t *testing.T) {
	if CRC16([]byte("123456789")) != 0x29B1 {
		t.Fatalf("%x", CRC16([]byte("123456789")))
	}
}

type pipeRW struct {
	io.Reader
	io.Writer
}

func TestLink(t *testing.T) {
	ar, bw := io.Pipe()
	br, aw := io.Pipe()
	a := New(pipeRW{ar, aw}, 0)
	b := New(pipeRW{br, bw}, 0)
	msgs := [][]byte{[]byte("hello"), {0, 0, 0}, bytes.Repeat([]byte{0xAB, 0}, 1000)}
	go func() {
		// Inject garbage between frames to exercise resync.
		for _, m := range msgs {
			a.Send(m)
			aw.Write([]byte{1, 2, 3, 0})
		}
	}()
	for _, want := range msgs {
		got, err := b.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("got % x want % x", got, want)
		}
	}
}
