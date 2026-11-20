package rtps

import (
	"encoding/binary"
	"net"

	"bitbucket.org/goros/transport"
)

// Protocol constants (DDS-RTPS 2.3).
const (
	protocolMajor = 2
	protocolMinor = 3

	headerSize = 20 // "RTPS" + version + vendor + prefix
	subHdrSize = 4

	// Submessage ids.
	smPad          = 0x01
	smAckNack      = 0x06
	smHeartbeat    = 0x07
	smGap          = 0x08
	smInfoTS       = 0x09
	smInfoSrc      = 0x0c
	smInfoReplyIP4 = 0x0d
	smInfoDst      = 0x0e
	smInfoReply    = 0x0f
	smNackFrag     = 0x12
	smHeartbeatFr  = 0x13
	smData         = 0x15
	smDataFrag     = 0x16

	flagEndian = 0x01
	// DATA flags
	flagInlineQoS = 0x02
	flagDataD     = 0x04
	flagDataK     = 0x08
	// DATA_FRAG flags
	flagFragK = 0x04
	// HEARTBEAT / ACKNACK
	flagFinal = 0x02
	// INFO_TS
	flagInvalidate = 0x02

	locatorUDPv4  = 1
	locatorKindSH = 16

	// Builtin endpoint set bits.
	beParticipantAnnouncer  = 1 << 0
	beParticipantDetector   = 1 << 1
	bePublicationAnnouncer  = 1 << 2
	bePublicationDetector   = 1 << 3
	beSubscriptionAnnouncer = 1 << 4
	beSubscriptionDetector  = 1 << 5
	beParticipantMsgWriter  = 1 << 10
	beParticipantMsgReader  = 1 << 11

	// Parameter ids.
	pidPad                         = 0x0000
	pidSentinel                    = 0x0001
	pidParticipantLeaseDuration    = 0x0002
	pidTopicName                   = 0x0005
	pidTypeName                    = 0x0007
	pidDomainID                    = 0x000f
	pidProtocolVersion             = 0x0015
	pidVendorID                    = 0x0016
	pidReliability                 = 0x001a
	pidLiveliness                  = 0x001b
	pidDurability                  = 0x001d
	pidDeadline                    = 0x0023
	pidUserData                    = 0x002c
	pidUnicastLocator              = 0x002f
	pidMulticastLocator            = 0x0030
	pidDefaultUnicastLocator       = 0x0031
	pidMetatrafficUnicastLocator   = 0x0032
	pidMetatrafficMulticastLocator = 0x0033
	pidHistory                     = 0x0040
	pidDefaultMulticastLocator     = 0x0048
	pidParticipantGUID             = 0x0050
	pidBuiltinEndpointSet          = 0x0058
	pidEndpointGUID                = 0x005a
	pidEntityName                  = 0x0062
	pidKeyHash                     = 0x0070
	pidStatusInfo                  = 0x0071
	pidDataRepresentation          = 0x0073
	pidRelatedSampleIdentity       = 0x0083
	pidFastRelatedSampleIdentity   = 0x800f // eProsima vendor id, written for Fast DDS < 2.12

	// DDS QoS kinds on the wire.
	reliabilityBestEffort = 1
	reliabilityReliable   = 2
	durabilityVolatile    = 0
	durabilityTransient   = 1
	historyKeepLast       = 0
	historyKeepAll        = 1
)

// Well known entity ids.
var (
	entityUnknown           = entityID{0, 0, 0, 0}
	entityParticipant       = entityID{0, 0, 1, 0xc1}
	entitySPDPWriter        = entityID{0, 1, 0, 0xc2}
	entitySPDPReader        = entityID{0, 1, 0, 0xc7}
	entitySEDPPubWriter     = entityID{0, 0, 3, 0xc2}
	entitySEDPPubReader     = entityID{0, 0, 3, 0xc7}
	entitySEDPSubWriter     = entityID{0, 0, 4, 0xc2}
	entitySEDPSubReader     = entityID{0, 0, 4, 0xc7}
	entityParticipantMsgWtr = entityID{0, 2, 0, 0xc2}
	entityParticipantMsgRdr = entityID{0, 2, 0, 0xc7}
)

// vendorID is an unassigned vendor id; the first byte of every registered
// id is 0x01 and 0x7a is far above the allocated range.
var vendorID = [2]byte{0x01, 0x7a}

type guidPrefix [12]byte
type entityID [4]byte

func makeGUID(p guidPrefix, e entityID) transport.GUID {
	var g transport.GUID
	copy(g[:12], p[:])
	copy(g[12:], e[:])
	return g
}

func prefixOf(g transport.GUID) (p guidPrefix) { copy(p[:], g[:12]); return }
func entityOf(g transport.GUID) (e entityID)   { copy(e[:], g[12:]); return }

// userWriter/userReader report the entity kind nibble of user endpoints.
func (e entityID) isWriter() bool { return e[3]&0x0f == 0x02 || e[3]&0x0f == 0x03 }
func (e entityID) isReader() bool { return e[3]&0x0f == 0x04 || e[3]&0x0f == 0x07 }

// locator is an RTPS Locator_t.
type locator struct {
	kind int32
	port uint32
	addr [16]byte
}

func udpv4Locator(ip net.IP, port int) locator {
	l := locator{kind: locatorUDPv4, port: uint32(port)}
	if ip4 := ip.To4(); ip4 != nil {
		copy(l.addr[12:], ip4)
	}
	return l
}

func (l locator) udpAddr() *net.UDPAddr {
	if l.kind != locatorUDPv4 {
		return nil
	}
	return &net.UDPAddr{IP: net.IPv4(l.addr[12], l.addr[13], l.addr[14], l.addr[15]), Port: int(l.port)}
}

// rtpsTime is a Time_t (seconds + 2^-32 fractions).
type rtpsTime struct {
	sec  int32
	frac uint32
}

var timeInfinite = rtpsTime{0x7fffffff, 0xffffffff}

func timeFromNanos(ns int64) rtpsTime {
	sec := ns / 1e9
	rem := ns - sec*1e9
	return rtpsTime{int32(sec), uint32((rem << 32) / 1e9)}
}

// --- little-endian message builder ----------------------------------------

type mbuf struct {
	b []byte
}

func (m *mbuf) reset()                { m.b = m.b[:0] }
func (m *mbuf) len() int              { return len(m.b) }
func (m *mbuf) raw(p []byte)          { m.b = append(m.b, p...) }
func (m *mbuf) u8(v uint8)            { m.b = append(m.b, v) }
func (m *mbuf) u16(v uint16)          { m.b = binary.LittleEndian.AppendUint16(m.b, v) }
func (m *mbuf) u32(v uint32)          { m.b = binary.LittleEndian.AppendUint32(m.b, v) }
func (m *mbuf) i32(v int32)           { m.u32(uint32(v)) }
func (m *mbuf) u64(v uint64)          { m.b = binary.LittleEndian.AppendUint64(m.b, v) }
func (m *mbuf) entity(e entityID)     { m.raw(e[:]) }
func (m *mbuf) prefix(p guidPrefix)   { m.raw(p[:]) }
func (m *mbuf) guid(g transport.GUID) { m.raw(g[:]) }
func (m *mbuf) sn(v int64) {
	m.i32(int32(v >> 32))
	m.u32(uint32(v))
}
func (m *mbuf) time(t rtpsTime) {
	m.i32(t.sec)
	m.u32(t.frac)
}
func (m *mbuf) locator(l locator) {
	m.i32(l.kind)
	m.u32(l.port)
	m.raw(l.addr[:])
}
func (m *mbuf) pad4() {
	for len(m.b)%4 != 0 {
		m.b = append(m.b, 0)
	}
}

// str writes a CDR string (length including NUL, chars, NUL).
func (m *mbuf) str(s string) {
	m.u32(uint32(len(s) + 1))
	m.b = append(m.b, s...)
	m.b = append(m.b, 0)
}

// header writes the RTPS message header.
func (m *mbuf) header(p guidPrefix) {
	m.raw([]byte{'R', 'T', 'P', 'S', protocolMajor, protocolMinor, vendorID[0], vendorID[1]})
	m.prefix(p)
}

// submsg writes a submessage header, runs body and patches the length.
func (m *mbuf) submsg(id, flags uint8, body func()) {
	start := len(m.b)
	m.u8(id)
	m.u8(flags | flagEndian)
	m.u16(0)
	body()
	m.pad4()
	n := len(m.b) - start - subHdrSize
	binary.LittleEndian.PutUint16(m.b[start+2:], uint16(n))
}

func (m *mbuf) infoDst(p guidPrefix) {
	m.submsg(smInfoDst, 0, func() { m.prefix(p) })
}

func (m *mbuf) infoTS(t rtpsTime) {
	m.submsg(smInfoTS, 0, func() { m.time(t) })
}

func (m *mbuf) heartbeat(reader, writer entityID, first, last int64, count int32, final bool) {
	var fl uint8
	if final {
		fl = flagFinal
	}
	m.submsg(smHeartbeat, fl, func() {
		m.entity(reader)
		m.entity(writer)
		m.sn(first)
		m.sn(last)
		m.i32(count)
	})
}

// snSet writes a SequenceNumberSet with the given base and set bits
// (offsets from base, each < 256).
func (m *mbuf) snSet(base int64, numBits uint32, bits []uint32) {
	m.sn(base)
	m.u32(numBits)
	for _, w := range bits {
		m.u32(w)
	}
}

func (m *mbuf) ackNack(reader, writer entityID, base int64, numBits uint32, bits []uint32, count int32, final bool) {
	var fl uint8
	if final {
		fl = flagFinal
	}
	m.submsg(smAckNack, fl, func() {
		m.entity(reader)
		m.entity(writer)
		m.snSet(base, numBits, bits)
		m.i32(count)
	})
}

// gap marks [start, end] as irrelevant.
func (m *mbuf) gap(reader, writer entityID, start, end int64) {
	m.submsg(smGap, 0, func() {
		m.entity(reader)
		m.entity(writer)
		m.sn(start)
		m.snSet(end+1, 0, nil)
	})
}

// data writes a DATA submessage. inlineQoS, when not nil, is a complete
// parameter list (with sentinel); payload, when not nil, is a serialized
// payload with its encapsulation header.
func (m *mbuf) data(reader, writer entityID, sn int64, inlineQoS, payload []byte, key bool) {
	var fl uint8
	if inlineQoS != nil {
		fl |= flagInlineQoS
	}
	if payload != nil {
		if key {
			fl |= flagDataK
		} else {
			fl |= flagDataD
		}
	}
	m.submsg(smData, fl, func() {
		m.u16(0)  // extraFlags
		m.u16(16) // octetsToInlineQos
		m.entity(reader)
		m.entity(writer)
		m.sn(sn)
		if inlineQoS != nil {
			m.raw(inlineQoS)
		}
		if payload != nil {
			m.raw(payload)
		}
	})
}

// dataFrag writes one DATA_FRAG submessage carrying fragments
// [startFrag, startFrag+n) of a sample.
func (m *mbuf) dataFrag(reader, writer entityID, sn int64, startFrag uint32, n uint16, fragSize uint16, sampleSize uint32, inlineQoS, chunk []byte) {
	var fl uint8
	if inlineQoS != nil {
		fl |= flagInlineQoS
	}
	m.submsg(smDataFrag, fl, func() {
		m.u16(0)
		m.u16(28)
		m.entity(reader)
		m.entity(writer)
		m.sn(sn)
		m.u32(startFrag)
		m.u16(n)
		m.u16(fragSize)
		m.u32(sampleSize)
		if inlineQoS != nil {
			m.raw(inlineQoS)
		}
		m.raw(chunk)
	})
}

// --- parameter lists -------------------------------------------------------

// plist builds a little-endian ParameterList.
type plist struct {
	mbuf
}

// param appends one parameter; body writes the value.
func (p *plist) param(pid uint16, body func()) {
	start := len(p.b)
	p.u16(pid)
	p.u16(0)
	body()
	p.pad4()
	binary.LittleEndian.PutUint16(p.b[start+2:], uint16(len(p.b)-start-4))
}

func (p *plist) sentinel() {
	p.u16(pidSentinel)
	p.u16(0)
}

// encapsulated returns the list prefixed by a PL_CDR_LE encapsulation.
func (p *plist) encapsulated() []byte {
	out := make([]byte, 0, len(p.b)+4)
	out = append(out, 0x00, 0x03, 0x00, 0x00)
	return append(out, p.b...)
}

// bytes returns a copy of the bare list.
func (p *plist) bytes() []byte { return append([]byte(nil), p.b...) }

func (p *plist) guidParam(pid uint16, g transport.GUID) {
	p.param(pid, func() { p.guid(g) })
}
func (p *plist) strParam(pid uint16, s string) { p.param(pid, func() { p.str(s) }) }
func (p *plist) u32Param(pid uint16, v uint32) { p.param(pid, func() { p.u32(v) }) }
func (p *plist) locatorParam(pid uint16, l locator) {
	p.param(pid, func() { p.locator(l) })
}
func (p *plist) reliability(reliable bool) {
	p.param(pidReliability, func() {
		if reliable {
			p.u32(reliabilityReliable)
		} else {
			p.u32(reliabilityBestEffort)
		}
		p.time(rtpsTime{0, 100 * 4294967}) // max_blocking_time 100ms
	})
}
func (p *plist) durability(transientLocal bool) {
	p.param(pidDurability, func() {
		if transientLocal {
			p.u32(durabilityTransient)
		} else {
			p.u32(durabilityVolatile)
		}
	})
}
func (p *plist) history(keepAll bool, depth int32) {
	p.param(pidHistory, func() {
		if keepAll {
			p.u32(historyKeepAll)
		} else {
			p.u32(historyKeepLast)
		}
		p.i32(depth)
	})
}
func (p *plist) relatedSampleIdentity(g transport.GUID, sn int64) {
	for _, pid := range []uint16{pidRelatedSampleIdentity, pidFastRelatedSampleIdentity} {
		p.param(pid, func() {
			p.guid(g)
			p.sn(sn)
		})
	}
}

// --- reader ----------------------------------------------------------------

// rbuf reads integers in the endianness selected by the submessage flags.
type rbuf struct {
	b   []byte
	off int
	le  bool
	bad bool
}

func (r *rbuf) remaining() int { return len(r.b) - r.off }
func (r *rbuf) need(n int) bool {
	if r.off+n > len(r.b) {
		r.bad = true
		return false
	}
	return true
}
func (r *rbuf) skip(n int) {
	if r.need(n) {
		r.off += n
	}
}
func (r *rbuf) raw(n int) []byte {
	if !r.need(n) {
		return nil
	}
	v := r.b[r.off : r.off+n]
	r.off += n
	return v
}
func (r *rbuf) u8() uint8 {
	if !r.need(1) {
		return 0
	}
	v := r.b[r.off]
	r.off++
	return v
}
func (r *rbuf) u16() uint16 {
	if !r.need(2) {
		return 0
	}
	var v uint16
	if r.le {
		v = binary.LittleEndian.Uint16(r.b[r.off:])
	} else {
		v = binary.BigEndian.Uint16(r.b[r.off:])
	}
	r.off += 2
	return v
}
func (r *rbuf) u32() uint32 {
	if !r.need(4) {
		return 0
	}
	var v uint32
	if r.le {
		v = binary.LittleEndian.Uint32(r.b[r.off:])
	} else {
		v = binary.BigEndian.Uint32(r.b[r.off:])
	}
	r.off += 4
	return v
}
func (r *rbuf) i32() int32 { return int32(r.u32()) }
func (r *rbuf) sn() int64 {
	hi := int64(r.i32())
	lo := int64(r.u32())
	return hi<<32 | lo
}
func (r *rbuf) entity() (e entityID)     { copy(e[:], r.raw(4)); return }
func (r *rbuf) prefix() (p guidPrefix)   { copy(p[:], r.raw(12)); return }
func (r *rbuf) guid() (g transport.GUID) { copy(g[:], r.raw(16)); return }
func (r *rbuf) time() rtpsTime           { return rtpsTime{r.i32(), r.u32()} }
func (r *rbuf) locator() locator {
	var l locator
	l.kind = r.i32()
	l.port = r.u32()
	copy(l.addr[:], r.raw(16))
	return l
}
func (r *rbuf) str() string {
	n := int(r.u32())
	if n <= 0 || !r.need(n) {
		return ""
	}
	s := r.b[r.off : r.off+n-1]
	r.off += n
	return string(s)
}
func (r *rbuf) align4() {
	for r.off%4 != 0 && r.off < len(r.b) {
		r.off++
	}
}

// param is one decoded parameter.
type param struct {
	pid   uint16
	value rbuf
}

// parseParams iterates a parameter list (without encapsulation) calling fn
// for each parameter until the sentinel. It returns the number of bytes
// consumed including the sentinel, and false on malformed input.
func parseParams(b []byte, le bool, fn func(p *param)) (int, bool) {
	r := rbuf{b: b, le: le}
	for r.remaining() >= 4 {
		pid := r.u16()
		n := int(r.u16())
		if pid == pidSentinel {
			return r.off, true
		}
		if !r.need(n) {
			return r.off, false
		}
		if pid != pidPad {
			p := param{pid: pid, value: rbuf{b: r.b[r.off : r.off+n], le: le}}
			fn(&p)
		}
		r.off += n
	}
	return r.off, true
}

// parsePLPayload handles an encapsulated PL_CDR payload.
func parsePLPayload(payload []byte, fn func(p *param)) bool {
	if len(payload) < 4 {
		return false
	}
	var ok bool
	switch binary.BigEndian.Uint16(payload) {
	case 0x0002: // PL_CDR_BE
		_, ok = parseParams(payload[4:], false, fn)
	case 0x0003: // PL_CDR_LE
		_, ok = parseParams(payload[4:], true, fn)
	}
	return ok
}
