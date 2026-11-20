package transport

import (
	"bytes"
	"testing"

	"bitbucket.org/goros/cdr"
)

func TestFrameRoundTrip(t *testing.T) {
	enc := cdr.NewEncoder()
	g := GUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	f := &Frame{Kind: KindData, From: g, Topic: "/chatter", Type: "std_msgs/msg/String", Payload: []byte{0, 1, 0, 0, 1, 0, 0, 0, 0}}
	b := f.Encode(enc)
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != KindData || got.From != g || got.Topic != "/chatter" || got.Type != f.Type || !bytes.Equal(got.Payload, f.Payload) {
		t.Fatalf("%+v", got)
	}

	h := &Frame{Kind: KindHello, From: g, Participant: &ParticipantInfo{Domain: 3, Nodes: []NodeInfo{{
		Name: "talker", Namespace: "/", Publishers: []EndpointInfo{{Name: "/chatter", Type: "std_msgs/msg/String", QoS: QoSInfo{1, 2, 1, 10}}},
		Services: []EndpointInfo{{Name: "/add", Type: "example_interfaces/srv/AddTwoInts"}},
	}}}}
	b = h.Encode(enc)
	got, err = Decode(append([]byte(nil), b...))
	if err != nil {
		t.Fatal(err)
	}
	if got.Participant.Domain != 3 || len(got.Participant.Nodes) != 1 || got.Participant.Nodes[0].Publishers[0].QoS.Depth != 10 || got.Participant.Nodes[0].Services[0].Name != "/add" {
		t.Fatalf("%+v", got.Participant)
	}

	r := &Frame{Kind: KindResponse, From: g, Topic: "/add", Target: g, Seq: 77, OK: false, Error: "boom"}
	got, err = Decode(r.Encode(enc))
	if err != nil || got.Seq != 77 || got.Error != "boom" || got.OK {
		t.Fatalf("%v %+v", err, got)
	}

	if _, err := Decode([]byte("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")); err != ErrBadMagic {
		t.Fatal(err)
	}
	if g.String() != "0102030405060708090a0b0c0d0e0f10" {
		t.Fatal(g.String())
	}
}
