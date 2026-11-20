package cdr

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	e := NewEncoder()
	e.Bool(true)
	e.Uint8(7)
	e.Int16(-3)
	e.Uint32(0xdeadbeef)
	e.Float64(3.5)
	e.String("hello")
	e.Int64(-42)
	e.Float32Seq([]float32{1, 2, 3})
	e.StringSeq([]string{"a", "", "cd"})
	e.WString("wïde")
	e.BytesSeq([]byte{9, 8, 7})

	d := NewDecoder(e.Bytes())
	if v := d.Bool(); !v {
		t.Fatal("bool")
	}
	if v := d.Uint8(); v != 7 {
		t.Fatal("uint8")
	}
	if v := d.Int16(); v != -3 {
		t.Fatal("int16")
	}
	if v := d.Uint32(); v != 0xdeadbeef {
		t.Fatal("uint32")
	}
	if v := d.Float64(); v != 3.5 {
		t.Fatal("float64")
	}
	if v := d.String(); v != "hello" {
		t.Fatalf("string %q", v)
	}
	if v := d.Int64(); v != -42 {
		t.Fatal("int64")
	}
	if v := d.Float32Seq(); len(v) != 3 || v[2] != 3 {
		t.Fatal("float32 seq")
	}
	if v := d.StringSeq(); len(v) != 3 || v[2] != "cd" || v[1] != "" {
		t.Fatal("string seq")
	}
	if v := d.WString(); v != "wïde" {
		t.Fatalf("wstring %q", v)
	}
	if v := d.BytesSeq(); !bytes.Equal(v, []byte{9, 8, 7}) {
		t.Fatal("bytes")
	}
	if d.Err() != nil {
		t.Fatal(d.Err())
	}
	if d.Remaining() != 0 {
		t.Fatalf("remaining %d", d.Remaining())
	}
}

// std_msgs/msg/String{data:"hello"} as serialized by rclcpp/Fast-CDR.
func TestStdMsgsStringWireCompat(t *testing.T) {
	want := []byte{0, 1, 0, 0, 6, 0, 0, 0, 'h', 'e', 'l', 'l', 'o', 0}
	e := NewEncoder()
	e.String("hello")
	if !bytes.Equal(e.Bytes(), want) {
		t.Fatalf("got % x want % x", e.Bytes(), want)
	}
	d := NewDecoder(want)
	if s := d.String(); s != "hello" || d.Err() != nil {
		t.Fatal(s, d.Err())
	}
}

// Alignment is measured from the start of the payload, not the buffer:
// a float64 immediately after the header must not be padded.
func TestAlignmentOrigin(t *testing.T) {
	e := NewEncoder()
	e.Float64(1.0)
	if e.Len() != 12 {
		t.Fatalf("len %d", e.Len())
	}
	e.Reset()
	e.Uint8(1)
	e.Uint32(2)
	// 4 header + 1 + 3 pad + 4
	if e.Len() != 12 {
		t.Fatalf("len %d", e.Len())
	}
}

func TestBigEndianDecode(t *testing.T) {
	// header(4) len(4) "hello\0"(6) pad-to-8(6) float64(8)
	data := []byte{0, 0, 0, 0, 0, 0, 0, 6, 'h', 'e', 'l', 'l', 'o', 0, 0, 0, 0, 0, 0, 0, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0}
	d := NewDecoder(data)
	if s := d.String(); s != "hello" {
		t.Fatal(s)
	}
	if v := d.Float64(); v != 1.0 {
		t.Fatal(v)
	}
	if d.Err() != nil {
		t.Fatal(d.Err())
	}
}

func TestErrors(t *testing.T) {
	d := NewDecoder([]byte{0, 1, 0, 0, 10, 0, 0, 0})
	_ = d.String()
	if d.Err() == nil {
		t.Fatal("expected short error")
	}
	d = NewDecoder([]byte{9, 9})
	if d.Err() != ErrBadHeader {
		t.Fatal("expected header error")
	}
	d = NewDecoder([]byte{0, 1, 0, 0, 0xff, 0xff, 0xff, 0xff})
	_ = d.Float64Seq()
	if d.Err() == nil {
		t.Fatal("expected length error")
	}
}
