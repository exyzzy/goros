package rtps

import (
	"bitbucket.org/goros/cdr"
	"bitbucket.org/goros/transport"
)

// ROS 2 publishes which nodes live in a DDS participant, and which
// endpoints belong to which node, on the ros_discovery_info topic
// (rmw_dds_common/msg/ParticipantEntitiesInfo). Without it a participant's
// endpoints are visible but its node names are not.
//
//	ParticipantEntitiesInfo { Gid gid; NodeEntitiesInfo[] node_entities_info_seq }
//	NodeEntitiesInfo { string node_namespace; string node_name; Gid[] reader_gid_seq; Gid[] writer_gid_seq }
//	Gid { uint8[16] data }   // 24 bytes before ROS 2 Iron

type nodeEntities struct {
	namespace string
	name      string
	readers   []transport.GUID
	writers   []transport.GUID
}

type participantEntities struct {
	gid   transport.GUID
	nodes []nodeEntities
}

const gidSize = 16

func encodeParticipantEntities(enc *cdr.Encoder, pe *participantEntities) []byte {
	enc.Reset()
	enc.RawBytes(pe.gid[:])
	enc.SeqLen(len(pe.nodes))
	for i := range pe.nodes {
		n := &pe.nodes[i]
		enc.String(n.namespace)
		enc.String(n.name)
		enc.SeqLen(len(n.readers))
		for _, g := range n.readers {
			enc.RawBytes(g[:])
		}
		enc.SeqLen(len(n.writers))
		for _, g := range n.writers {
			enc.RawBytes(g[:])
		}
	}
	return append([]byte(nil), enc.Bytes()...)
}

// decodeParticipantEntities accepts both the 16 and the legacy 24 byte Gid.
func decodeParticipantEntities(payload []byte) (*participantEntities, bool) {
	for _, size := range []int{16, 24} {
		if pe, ok := decodeParticipantEntitiesSized(payload, size); ok {
			return pe, true
		}
	}
	return nil, false
}

func decodeParticipantEntitiesSized(payload []byte, size int) (*participantEntities, bool) {
	d := cdr.NewDecoder(payload)
	pe := &participantEntities{}
	gid := make([]byte, size)
	d.RawBytes(gid)
	copy(pe.gid[:], gid)
	n := d.SeqLen(8)
	if d.Err() != nil || n > 4096 {
		return nil, false
	}
	pe.nodes = make([]nodeEntities, n)
	readGIDs := func() []transport.GUID {
		k := d.SeqLen(size)
		if d.Err() != nil || k > 65536 {
			return nil
		}
		out := make([]transport.GUID, k)
		for i := range out {
			d.RawBytes(gid)
			copy(out[i][:], gid)
		}
		return out
	}
	for i := range pe.nodes {
		pe.nodes[i].namespace = d.String()
		pe.nodes[i].name = d.String()
		pe.nodes[i].readers = readGIDs()
		pe.nodes[i].writers = readGIDs()
		if d.Err() != nil {
			return nil, false
		}
	}
	return pe, d.Err() == nil && d.Remaining() < 8
}
