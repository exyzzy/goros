package rtps

import (
	"testing"

	"bitbucket.org/goros/cdr"
	"bitbucket.org/goros/transport"
)

func TestNames(t *testing.T) {
	cases := []struct{ ros, dds string }{
		{"std_msgs/msg/String", "std_msgs::msg::dds_::String_"},
		{"example_interfaces/srv/AddTwoInts_Request", "example_interfaces::srv::dds_::AddTwoInts_Request_"},
	}
	for _, c := range cases {
		if got := ddsType(c.ros); got != c.dds {
			t.Errorf("ddsType(%q) = %q", c.ros, got)
		}
		if got := rosType(c.dds); got != c.ros {
			t.Errorf("rosType(%q) = %q", c.dds, got)
		}
	}
	if ddsTopic("/chatter") != "rt/chatter" {
		t.Error("topic prefix")
	}
	if ddsRequestTopic("/add_two_ints") != "rq/add_two_intsRequest" || ddsReplyTopic("/ns/x") != "rr/ns/xReply" {
		t.Error("service topics")
	}
	if n, ok := rosTopic("rt/a/b"); !ok || n != "/a/b" {
		t.Error("rosTopic")
	}
	if n, req, ok := rosService("rq/ns/add_two_intsRequest"); !ok || !req || n != "/ns/add_two_ints" {
		t.Error("rosService request")
	}
	if n, req, ok := rosService("rr/add_two_intsReply"); !ok || req || n != "/add_two_ints" {
		t.Error("rosService reply")
	}
	if rosServiceType(rosType("example_interfaces::srv::dds_::AddTwoInts_Response_")) != "example_interfaces/srv/AddTwoInts" {
		t.Error("service type")
	}
}

func TestParameterListRoundTrip(t *testing.T) {
	var pl plist
	g := transport.GUID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 0, 0, 1, 0xc1}
	pl.guidParam(pidParticipantGUID, g)
	pl.strParam(pidTopicName, "rt/chatter")
	pl.u32Param(pidBuiltinEndpointSet, 0x3f)
	pl.reliability(true)
	pl.locatorParam(pidMetatrafficUnicastLocator, udpv4Locator([]byte{10, 0, 0, 7}, 7411))
	pl.sentinel()
	enc := pl.encapsulated()
	if enc[0] != 0 || enc[1] != 3 {
		t.Fatal("bad encapsulation")
	}
	seen := map[uint16]bool{}
	ok := parsePLPayload(enc, func(p *param) {
		seen[p.pid] = true
		switch p.pid {
		case pidParticipantGUID:
			if p.value.guid() != g {
				t.Error("guid mismatch")
			}
		case pidTopicName:
			if s := p.value.str(); s != "rt/chatter" {
				t.Errorf("topic %q", s)
			}
		case pidBuiltinEndpointSet:
			if p.value.u32() != 0x3f {
				t.Error("builtin set")
			}
		case pidReliability:
			if p.value.u32() != reliabilityReliable {
				t.Error("reliability")
			}
		case pidMetatrafficUnicastLocator:
			l := p.value.locator()
			if a := l.udpAddr(); a == nil || a.Port != 7411 || a.IP.String() != "10.0.0.7" {
				t.Errorf("locator %v", a)
			}
		}
	})
	if !ok || len(seen) != 5 {
		t.Fatalf("parse ok=%v seen=%v", ok, seen)
	}
	if len(enc)%4 != 0 {
		t.Error("parameter list not 4-byte aligned")
	}
}

func TestSubmessageParsing(t *testing.T) {
	tr := &Transport{prefix: guidPrefix{1, 2, 3}, proxies: map[transport.GUID]*writerProxy{}, writersByGUID: map[transport.GUID]*writer{}, frags: map[fragKey]*fragBuf{}}
	remote := guidPrefix{9, 9, 9}
	var m mbuf
	m.header(remote)
	m.infoDst(tr.prefix)
	m.infoTS(timeFromNanos(1_700_000_000_123_456_789))
	var pl plist
	pl.relatedSampleIdentity(makeGUID(remote, entityID{0, 0, 1, 3}), 42)
	pl.sentinel()
	m.data(entityID{0, 0, 2, 4}, entityID{0, 0, 1, 3}, 7, pl.bytes(), []byte{0, 1, 0, 0, 5, 0, 0, 0}, false)
	m.heartbeat(entityUnknown, entityID{0, 0, 1, 3}, 1, 7, 3, false)

	// A proxy for the writer so the DATA is accepted; capture via a reader.
	rw := &remoteWriter{remoteEndpoint: remoteEndpoint{guid: makeGUID(remote, entityID{0, 0, 1, 3}), kind: ekTopic, rosName: "/x", rosType: "std_msgs/msg/String"}, part: &remoteParticipant{prefix: remote}}
	p := &writerProxy{guid: rw.guid, rw: rw, part: rw.part, reliable: true, readers: map[*reader]struct{}{{reliable: true, tl: true}: {}}, missing: map[int64]struct{}{}}
	tr.proxies[p.guid] = p
	tr.out = make(chan []byte, 8)
	tr.enc = cdr.NewEncoder()
	tr.handlePacket(m.b, nil)
	select {
	case b := <-tr.out:
		f, err := transport.Decode(b)
		if err != nil || f.Kind != transport.KindData || f.Topic != "/x" || len(f.Payload) != 8 {
			t.Fatalf("bad frame %+v %v", f, err)
		}
	default:
		t.Fatal("no frame emitted")
	}
	if p.expected != 8 || len(p.missing) != 6 {
		t.Errorf("reliability state: expected=%d missing=%d", p.expected, len(p.missing))
	}
	// Duplicate must be dropped.
	tr.handlePacket(m.b, nil)
	if len(tr.out) != 0 {
		t.Error("duplicate delivered")
	}
}

func TestDiscoveryInfoRoundTrip(t *testing.T) {
	pe := &participantEntities{gid: transport.GUID{1}}
	pe.nodes = append(pe.nodes, nodeEntities{namespace: "/", name: "talker", writers: []transport.GUID{{2}, {3}}}, nodeEntities{namespace: "/ns", name: "l", readers: []transport.GUID{{4}}})
	b := encodeParticipantEntities(cdr.NewEncoder(), pe)
	got, ok := decodeParticipantEntities(b)
	if !ok || len(got.nodes) != 2 || got.nodes[0].name != "talker" || len(got.nodes[0].writers) != 2 || got.nodes[1].namespace != "/ns" || got.nodes[1].readers[0] != (transport.GUID{4}) {
		t.Fatalf("round trip failed: %+v ok=%v", got, ok)
	}
}
