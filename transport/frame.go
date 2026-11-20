package transport

import (
	"errors"

	"bitbucket.org/goros/cdr"
)

// Kind identifies the purpose of a frame.
type Kind uint8

const (
	KindHello    Kind = 1 // periodic participant/graph announcement
	KindBye      Kind = 2 // participant leaving
	KindData     Kind = 3 // topic sample
	KindRequest  Kind = 4 // service request
	KindResponse Kind = 5 // service response
	// KindDataReplay is a Data frame re-sent from a transient-local
	// publisher's history for a late-joining subscription. Receivers treat
	// it exactly like KindData; transports that implement durability
	// themselves (RTPS) drop it.
	KindDataReplay Kind = 6
)

// Protocol constants.
const (
	Magic0  = 'G'
	Magic1  = 'R'
	Version = 1
)

var (
	ErrBadMagic   = errors.New("transport: bad frame magic")
	ErrBadVersion = errors.New("transport: unsupported frame version")
	ErrBadKind    = errors.New("transport: unknown frame kind")
)

// GUID uniquely identifies a participant (one goros Context). It mirrors the
// 16-byte DDS GUID used by rmw.
type GUID [16]byte

func (g GUID) IsZero() bool { return g == GUID{} }

// String renders the GUID as lowercase hex.
func (g GUID) String() string {
	const hexd = "0123456789abcdef"
	var out [32]byte
	for i, b := range g {
		out[i*2] = hexd[b>>4]
		out[i*2+1] = hexd[b&0xf]
	}
	return string(out[:])
}

// QoSInfo is the subset of QoS carried in discovery.
type QoSInfo struct {
	Reliability uint8
	Durability  uint8
	History     uint8
	Depth       uint32
}

// EndpointInfo describes one publisher/subscription/service/client.
type EndpointInfo struct {
	Name string // fully-qualified topic or service name
	Type string // ROS type name
	QoS  QoSInfo
}

// NodeInfo describes one node of a participant.
type NodeInfo struct {
	Name          string
	Namespace     string
	Publishers    []EndpointInfo
	Subscriptions []EndpointInfo
	Services      []EndpointInfo
	Clients       []EndpointInfo
}

// ParticipantInfo is the payload of a Hello frame.
type ParticipantInfo struct {
	Domain uint32
	Nodes  []NodeInfo
}

// Frame is the decoded form of every message exchanged between participants.
type Frame struct {
	Kind Kind
	From GUID

	// Data / Request / Response
	Topic   string // topic or service name
	Type    string // ROS type name (message or service)
	Payload []byte // CDR-serialized message (with encapsulation header)

	// Request / Response
	Target GUID   // Response: the requesting participant
	Seq    uint64 // request sequence number (per client participant)
	OK     bool   // Response: handler succeeded
	Error  string // Response: handler error text when !OK

	// Hello
	Participant *ParticipantInfo
}

func encodeEndpoints(e *cdr.Encoder, eps []EndpointInfo) {
	e.SeqLen(len(eps))
	for i := range eps {
		e.String(eps[i].Name)
		e.String(eps[i].Type)
		e.Uint8(eps[i].QoS.Reliability)
		e.Uint8(eps[i].QoS.Durability)
		e.Uint8(eps[i].QoS.History)
		e.Uint32(eps[i].QoS.Depth)
	}
}

func decodeEndpoints(d *cdr.Decoder) []EndpointInfo {
	n := d.SeqLen(8)
	if d.Err() != nil {
		return nil
	}
	eps := make([]EndpointInfo, n)
	for i := range eps {
		eps[i].Name = d.String()
		eps[i].Type = d.String()
		eps[i].QoS.Reliability = d.Uint8()
		eps[i].QoS.Durability = d.Uint8()
		eps[i].QoS.History = d.Uint8()
		eps[i].QoS.Depth = d.Uint32()
	}
	return eps
}

// Encode serializes the frame with enc (reset first) and returns the bytes,
// valid until enc is next used.
func (f *Frame) Encode(enc *cdr.Encoder) []byte {
	enc.Reset()
	enc.RawBytes(f.From[:])
	switch f.Kind {
	case KindData, KindDataReplay:
		enc.String(f.Topic)
		enc.String(f.Type)
		enc.BytesSeq(f.Payload)
	case KindRequest:
		enc.String(f.Topic)
		enc.String(f.Type)
		enc.Uint64(f.Seq)
		enc.BytesSeq(f.Payload)
	case KindResponse:
		enc.String(f.Topic)
		enc.RawBytes(f.Target[:])
		enc.Uint64(f.Seq)
		enc.Bool(f.OK)
		enc.String(f.Error)
		enc.BytesSeq(f.Payload)
	case KindHello:
		p := f.Participant
		if p == nil {
			p = &ParticipantInfo{}
		}
		enc.Uint32(p.Domain)
		enc.SeqLen(len(p.Nodes))
		for i := range p.Nodes {
			n := &p.Nodes[i]
			enc.String(n.Name)
			enc.String(n.Namespace)
			encodeEndpoints(enc, n.Publishers)
			encodeEndpoints(enc, n.Subscriptions)
			encodeEndpoints(enc, n.Services)
			encodeEndpoints(enc, n.Clients)
		}
	case KindBye:
	}
	// Prefix with magic/version/kind. The CDR body already starts with its
	// own 4-byte encapsulation header so we splice a 4-byte prefix in front;
	// the body's alignment origin is unaffected because 4 % 8 == 4 keeps
	// 8-byte alignment consistent only if we account for it below.
	b := enc.Bytes()
	out := make([]byte, 0, len(b)+4)
	out = append(out, Magic0, Magic1, Version, byte(f.Kind))
	return append(out, b...)
}

// Decode parses a frame produced by Encode.
func Decode(buf []byte) (*Frame, error) {
	if len(buf) < 4+cdr.HeaderSize+16 {
		return nil, cdr.ErrShort
	}
	if buf[0] != Magic0 || buf[1] != Magic1 {
		return nil, ErrBadMagic
	}
	if buf[2] != Version {
		return nil, ErrBadVersion
	}
	f := &Frame{Kind: Kind(buf[3])}
	d := cdr.NewDecoder(buf[4:])
	d.RawBytes(f.From[:])
	switch f.Kind {
	case KindData, KindDataReplay:
		f.Topic = d.String()
		f.Type = d.String()
		f.Payload = d.BytesSeq()
	case KindRequest:
		f.Topic = d.String()
		f.Type = d.String()
		f.Seq = d.Uint64()
		f.Payload = d.BytesSeq()
	case KindResponse:
		f.Topic = d.String()
		d.RawBytes(f.Target[:])
		f.Seq = d.Uint64()
		f.OK = d.Bool()
		f.Error = d.String()
		f.Payload = d.BytesSeq()
	case KindHello:
		p := &ParticipantInfo{}
		p.Domain = d.Uint32()
		n := d.SeqLen(8)
		if d.Err() == nil {
			p.Nodes = make([]NodeInfo, n)
			for i := range p.Nodes {
				nd := &p.Nodes[i]
				nd.Name = d.String()
				nd.Namespace = d.String()
				nd.Publishers = decodeEndpoints(d)
				nd.Subscriptions = decodeEndpoints(d)
				nd.Services = decodeEndpoints(d)
				nd.Clients = decodeEndpoints(d)
			}
		}
		f.Participant = p
	case KindBye:
	default:
		return nil, ErrBadKind
	}
	if err := d.Err(); err != nil {
		return nil, err
	}
	return f, nil
}
