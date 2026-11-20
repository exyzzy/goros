package rtps

import (
	"encoding/binary"
	"net"
	"sort"
	"strconv"
	"time"

	"bitbucket.org/goros/transport"
)

// This file holds the DDS-facing half: incoming RTPS messages, remote
// participants and their endpoints, the reliable-reader machinery, and the
// synthesis of goros Hello frames from what was discovered.

type remoteParticipant struct {
	prefix   guidPrefix
	guid     transport.GUID
	vendor   [2]byte
	name     string
	metaAddr *net.UDPAddr
	userAddr *net.UDPAddr
	metaLocs []locator
	userLocs []locator
	src      net.IP
	builtin  uint32
	lease    time.Duration
	lastSeen time.Time
	writers  map[entityID]*remoteWriter
	readers  map[entityID]*remoteReader
	entities *participantEntities
	dirty    bool
}

type remoteEndpoint struct {
	guid     transport.GUID
	topic    string
	typ      string
	reliable bool
	tl       bool
	keepAll  bool
	depth    int
	kind     endpointKind
	rosName  string // demangled topic or service name
	rosType  string // demangled message or service type
}

type remoteWriter struct {
	remoteEndpoint
	part *remoteParticipant
}

type remoteReader struct {
	remoteEndpoint
	part *remoteParticipant
	addr *net.UDPAddr
}

// writerProxy tracks what we received from one remote writer, on behalf
// of every local reader matched to it.
type writerProxy struct {
	guid         transport.GUID
	part         *remoteParticipant
	rw           *remoteWriter // nil for builtin
	builtin      bool
	readerEntity entityID // builtin: our SEDP reader
	reliable     bool
	tl           bool
	readers      map[*reader]struct{}
	aligned      bool
	expected     int64
	lowest       int64 // lowest sequence number accepted so far
	missing      map[int64]struct{}
	ackCount     int32
	lastHB       int32
	seenHB       bool
}

type inlineQoS struct {
	related     transport.GUID
	relatedSN   int64
	haveRelated bool
	keyHash     transport.GUID
	disposed    bool
}

type fragKey struct {
	writer transport.GUID
	sn     int64
}

type fragBuf struct {
	data     []byte
	have     []bool
	got      int
	fragSize int
	qos      inlineQoS
	deadline time.Time
	key      bool
}

const maxMissing = 1024

// --- packet parsing --------------------------------------------------------

func (t *Transport) handlePacket(b []byte, from *net.UDPAddr) {
	if len(b) < headerSize || b[0] != 'R' || b[1] != 'T' || b[2] != 'P' || b[3] != 'S' || b[4] != protocolMajor {
		return
	}
	var srcPrefix guidPrefix
	copy(srcPrefix[:], b[8:20])
	if srcPrefix == t.prefix {
		return
	}
	vendor := [2]byte{b[6], b[7]}
	off := headerSize
	skip := false
	if t.opts.Debug != nil {
		ids := ""
		for o := headerSize; o+subHdrSize <= len(b); {
			id, fl := b[o], b[o+1]
			var n int
			if fl&flagEndian != 0 {
				n = int(binary.LittleEndian.Uint16(b[o+2:]))
			} else {
				n = int(binary.BigEndian.Uint16(b[o+2:]))
			}
			ids += " " + strconv.FormatUint(uint64(id), 16)
			if id == smInfoDst {
				var p guidPrefix
				copy(p[:], b[o+subHdrSize:])
				ids += "(" + p.String() + ")"
			}
			if id == smData && o+subHdrSize+12 <= len(b) {
				var e entityID
				copy(e[:], b[o+subHdrSize+8:])
				ids += "(w=" + transport.GUID(makeGUID(srcPrefix, e)).String()[24:] + ")"
			}
			if n == 0 {
				break
			}
			o += subHdrSize + n
		}
		t.debugf("pkt from ", from.String(), " ", srcPrefix.String(), " submsgs:", ids)
	}
	for off+subHdrSize <= len(b) {
		id, flags := b[off], b[off+1]
		le := flags&flagEndian != 0
		var n int
		if le {
			n = int(binary.LittleEndian.Uint16(b[off+2:]))
		} else {
			n = int(binary.BigEndian.Uint16(b[off+2:]))
		}
		start := off + subHdrSize
		end := start + n
		if n == 0 && (id == smData || id == smDataFrag) {
			end = len(b)
		}
		if end > len(b) {
			return
		}
		r := rbuf{b: b[start:end], le: le}
		switch id {
		case smInfoDst:
			p := r.prefix()
			skip = p != (guidPrefix{}) && p != t.prefix
		case smInfoTS, smInfoSrc, smInfoReply, smInfoReplyIP4, smPad:
		default:
			if !skip {
				t.handleSubmsg(id, flags, &r, srcPrefix, vendor, from)
			}
		}
		off = end
	}
}

func (t *Transport) handleSubmsg(id, flags uint8, r *rbuf, src guidPrefix, vendor [2]byte, from *net.UDPAddr) {
	switch id {
	case smData:
		r.u16() // extraFlags
		otq := int(r.u16())
		readerID := r.entity()
		writerID := r.entity()
		sn := r.sn()
		if r.bad || 4+otq > len(r.b) {
			return
		}
		r.off = 4 + otq
		var iq inlineQoS
		if flags&flagInlineQoS != 0 {
			n, ok := parseInlineQoS(r.b[r.off:], r.le, &iq)
			if !ok {
				return
			}
			r.off += n
		}
		var payload []byte
		if flags&(flagDataD|flagDataK) != 0 {
			payload = r.b[r.off:]
		}
		t.dispatchSample(src, vendor, from, writerID, readerID, sn, &iq, payload, flags&flagDataK != 0)
	case smDataFrag:
		r.u16()
		otq := int(r.u16())
		readerID := r.entity()
		writerID := r.entity()
		sn := r.sn()
		startFrag := r.u32()
		nFrags := int(r.u16())
		fsize := int(r.u16())
		total := int(r.u32())
		if r.bad || 4+otq > len(r.b) || fsize == 0 || total == 0 || total > 64<<20 {
			return
		}
		r.off = 4 + otq
		var iq inlineQoS
		if flags&flagInlineQoS != 0 {
			n, ok := parseInlineQoS(r.b[r.off:], r.le, &iq)
			if !ok {
				return
			}
			r.off += n
		}
		if payload, ok := t.assemble(makeGUID(src, writerID), sn, int(startFrag), nFrags, fsize, total, &iq, flags&flagFragK != 0, r.b[r.off:]); ok {
			t.dispatchSample(src, vendor, from, writerID, readerID, sn, &iq, payload, flags&flagFragK != 0)
		}
	case smHeartbeat:
		r.entity()
		writerID := r.entity()
		first := r.sn()
		last := r.sn()
		count := r.i32()
		if r.bad {
			return
		}
		t.onHeartbeat(makeGUID(src, writerID), first, last, count, flags&flagFinal != 0)
	case smAckNack:
		readerID := r.entity()
		writerID := r.entity()
		base := r.sn()
		numBits := r.u32()
		if r.bad || numBits > 256 {
			return
		}
		words := make([]uint32, (numBits+31)/32)
		for i := range words {
			words[i] = r.u32()
		}
		count := r.i32()
		if r.bad {
			return
		}
		w := t.writersByGUID[makeGUID(t.prefix, writerID)]
		if w == nil {
			t.debugf("ACKNACK for unknown writer")
			return
		}
		p := w.proxies[makeGUID(src, readerID)]
		if p == nil {
			t.debugf("ACKNACK from unknown reader ", makeGUID(src, readerID).String(), " for ", w.topic)
			return
		}
		t.debugf("ACKNACK base=", itoa(int(base)), " bits=", itoa(int(numBits)), " for ", w.topic)
		t.onAckNack(w, p, base, numBits, words, count, flags&flagFinal != 0)
	case smGap:
		r.entity()
		writerID := r.entity()
		gapStart := r.sn()
		base := r.sn()
		numBits := r.u32()
		if r.bad || numBits > 256 {
			return
		}
		words := make([]uint32, (numBits+31)/32)
		for i := range words {
			words[i] = r.u32()
		}
		if p := t.proxies[makeGUID(src, writerID)]; p != nil {
			p.gap(gapStart, base, numBits, words)
		}
	}
}

func parseInlineQoS(b []byte, le bool, iq *inlineQoS) (int, bool) {
	return parseParams(b, le, func(p *param) {
		switch p.pid {
		case pidRelatedSampleIdentity, pidFastRelatedSampleIdentity:
			g := p.value.guid()
			sn := p.value.sn()
			if !p.value.bad {
				iq.related, iq.relatedSN, iq.haveRelated = g, sn, true
			}
		case pidKeyHash:
			iq.keyHash = p.value.guid()
		case pidStatusInfo:
			v := p.value.raw(4)
			if len(v) == 4 && (v[0]|v[1]|v[2]|v[3])&0x3 != 0 {
				iq.disposed = true
			}
		}
	})
}

// assemble collects DATA_FRAG pieces; it returns the sample once complete.
func (t *Transport) assemble(wguid transport.GUID, sn int64, startFrag, nFrags, fsize, total int, iq *inlineQoS, key bool, data []byte) ([]byte, bool) {
	k := fragKey{wguid, sn}
	fb := t.frags[k]
	if fb == nil {
		if len(t.frags) > 256 {
			return nil, false
		}
		nf := (total + fsize - 1) / fsize
		fb = &fragBuf{data: make([]byte, total), have: make([]bool, nf), fragSize: fsize, key: key}
		t.frags[k] = fb
	}
	fb.deadline = time.Now().Add(fragTimeout)
	if iq.haveRelated || iq.disposed {
		fb.qos = *iq
	}
	for i := 0; i < nFrags; i++ {
		idx := startFrag - 1 + i
		if idx < 0 || idx >= len(fb.have) {
			break
		}
		off := idx * fsize
		n := fsize
		if off+n > total {
			n = total - off
		}
		if len(data) < n {
			break
		}
		if !fb.have[idx] {
			copy(fb.data[off:], data[:n])
			fb.have[idx] = true
			fb.got++
		}
		data = data[n:]
	}
	if fb.got < len(fb.have) {
		return nil, false
	}
	delete(t.frags, k)
	*iq = fb.qos
	return fb.data, true
}

// --- sample dispatch -------------------------------------------------------

func (t *Transport) dispatchSample(src guidPrefix, vendor [2]byte, from *net.UDPAddr, writerID, readerID entityID, sn int64, iq *inlineQoS, payload []byte, key bool) {
	if writerID == entitySPDPWriter {
		t.handleSPDP(src, vendor, from, iq, payload)
		return
	}
	p := t.proxies[makeGUID(src, writerID)]
	if p == nil {
		t.debugf("DATA from unknown writer ", makeGUID(src, writerID).String())
		return
	}
	if !p.accept(sn) {
		t.debugf("DATA dup sn=", itoa(int(sn)), " from ", p.guid.String())
		return
	}
	t.debugf("DATA sn=", itoa(int(sn)), " from ", p.guid.String())
	if p.builtin {
		t.handleSEDP(p, iq, payload, key)
		return
	}
	t.deliverUser(p, iq, payload)
}

// accept does reliable-reader bookkeeping; false means duplicate.
func (p *writerProxy) accept(sn int64) bool {
	if !p.aligned {
		p.aligned = true
		p.expected = sn
		p.lowest = sn
	}
	if sn < p.lowest {
		p.lowest = sn
	}
	switch {
	case sn < p.expected:
		if _, ok := p.missing[sn]; ok {
			delete(p.missing, sn)
			return true
		}
		return false
	case sn == p.expected:
		p.expected++
		return true
	default:
		if p.reliable {
			for s := p.expected; s < sn && len(p.missing) < maxMissing; s++ {
				p.missing[s] = struct{}{}
			}
		}
		p.expected = sn + 1
		return true
	}
}

func (p *writerProxy) wantsHistory() bool {
	if p.builtin {
		return true
	}
	for r := range p.readers {
		if r.tl {
			return true
		}
	}
	return false
}

func (p *writerProxy) anyReliable() bool {
	if p.builtin {
		return true
	}
	for r := range p.readers {
		if r.reliable {
			return true
		}
	}
	return false
}

func (p *writerProxy) gap(gapStart, base int64, numBits uint32, words []uint32) {
	for s := gapStart; s < base; s++ {
		delete(p.missing, s)
	}
	for i := uint32(0); i < numBits && i < uint32(len(words))*32; i++ {
		if words[i/32]&(1<<(31-i%32)) != 0 {
			delete(p.missing, base+int64(i))
		}
	}
	if p.expected >= gapStart && p.expected < base {
		p.expected = base
	}
	if !p.aligned {
		p.aligned, p.expected = true, base
	}
}

func (t *Transport) onHeartbeat(wguid transport.GUID, first, last int64, count int32, final bool) {
	p := t.proxies[wguid]
	if p == nil {
		t.debugf("HEARTBEAT from unknown writer ", wguid.String())
		return
	}
	t.debugf("HEARTBEAT ", itoa(int(first)), "..", itoa(int(last)), " from ", wguid.String(), " expected=", itoa(int(p.expected)))
	if p.seenHB && count <= p.lastHB {
		return
	}
	p.seenHB, p.lastHB = true, count
	if !p.aligned {
		p.aligned = true
		if p.wantsHistory() {
			p.expected = first
		} else {
			p.expected = last + 1
		}
		p.lowest = p.expected
	} else if p.reliable && first < p.lowest && p.wantsHistory() {
		// Samples arrived before we knew where the writer's history
		// starts; fetch the older part now.
		for s := first; s < p.lowest && len(p.missing) < maxMissing; s++ {
			p.missing[s] = struct{}{}
		}
		p.lowest = first
	}
	if first > p.expected {
		for s := range p.missing {
			if s < first {
				delete(p.missing, s)
			}
		}
		p.expected = first
	}
	if p.reliable {
		for s := p.expected; s <= last && len(p.missing) < maxMissing; s++ {
			p.missing[s] = struct{}{}
		}
	}
	if last+1 > p.expected {
		p.expected = last + 1
	}
	if !p.reliable || !p.anyReliable() {
		return
	}
	if len(p.missing) > 0 || !final {
		t.sendAckNack(p)
	}
}

// sendAckNack tells the writer what we have and what we miss, once per
// matched reliable reader.
func (t *Transport) sendAckNack(p *writerProxy) {
	base := p.expected
	for s := range p.missing {
		if s < base {
			base = s
		}
	}
	if p.expected-base > 256 {
		base = p.expected - 256
		for s := range p.missing {
			if s < base {
				delete(p.missing, s)
			}
		}
	}
	numBits := uint32(p.expected - base)
	words := make([]uint32, (numBits+31)/32)
	for s := range p.missing {
		i := uint32(s - base)
		words[i/32] |= 1 << (31 - i%32)
	}
	p.ackCount++
	final := len(p.missing) == 0
	t.mb.reset()
	t.mb.header(t.prefix)
	t.mb.infoDst(p.part.prefix)
	wid := entityOf(p.guid)
	var addr *net.UDPAddr
	if p.builtin {
		t.mb.ackNack(p.readerEntity, wid, base, numBits, words, p.ackCount, final)
		addr = p.part.metaAddr
	} else {
		for r := range p.readers {
			if r.reliable {
				t.mb.ackNack(r.entity, wid, base, numBits, words, p.ackCount, final)
			}
		}
		addr = p.part.userAddr
	}
	t.sendTo(addr, t.mb.b)
}

// --- SPDP ------------------------------------------------------------------

func (t *Transport) handleSPDP(src guidPrefix, vendor [2]byte, from *net.UDPAddr, iq *inlineQoS, payload []byte) {
	if payload == nil || iq.disposed {
		p := src
		if !iq.keyHash.IsZero() {
			p = prefixOf(iq.keyHash)
		}
		if _, ok := t.remotes[p]; ok {
			t.debugf("participant ", p.String(), " disposed")
			t.removeRemote(p)
		}
		return
	}
	var guid transport.GUID
	var builtin uint32
	var metaLocs, userLocs []locator
	lease := spdpLeaseDefault
	name := ""
	domainOK := true
	ok := parsePLPayload(payload, func(p *param) {
		switch p.pid {
		case pidParticipantGUID:
			guid = p.value.guid()
		case pidBuiltinEndpointSet:
			builtin = p.value.u32()
		case pidMetatrafficUnicastLocator:
			if l := p.value.locator(); l.kind == locatorUDPv4 {
				metaLocs = append(metaLocs, l)
			}
		case pidDefaultUnicastLocator:
			if l := p.value.locator(); l.kind == locatorUDPv4 {
				userLocs = append(userLocs, l)
			}
		case pidParticipantLeaseDuration:
			tm := p.value.time()
			if tm != timeInfinite && tm.sec >= 0 {
				lease = time.Duration(tm.sec)*time.Second + time.Duration(uint64(tm.frac)*1e9>>32)
			}
		case pidDomainID:
			if int(p.value.u32()) != t.domain {
				domainOK = false
			}
		case pidEntityName:
			name = p.value.str()
		}
	})
	if !ok || !domainOK {
		return
	}
	prefix := prefixOf(guid)
	if guid.IsZero() {
		prefix = src
	}
	if prefix == t.prefix {
		return
	}
	if lease < 3*time.Second {
		lease = 3 * time.Second
	}
	rp := t.remotes[prefix]
	if rp != nil {
		rp.lastSeen = time.Now()
		rp.lease = lease
		return
	}
	if len(metaLocs) == 0 {
		return
	}
	rp = &remoteParticipant{
		prefix: prefix, guid: makeGUID(prefix, entityParticipant), vendor: vendor, name: name,
		metaLocs: metaLocs, userLocs: userLocs, src: from.IP, builtin: builtin, lease: lease, lastSeen: time.Now(),
		writers: map[entityID]*remoteWriter{}, readers: map[entityID]*remoteReader{},
	}
	rp.metaAddr = pickAddr(metaLocs, from.IP, nil)
	rp.userAddr = pickAddr(userLocs, from.IP, rp.metaAddr)
	t.remotes[prefix] = rp
	t.debugf("participant ", rp.guid.String(), " (", name, ") discovered at ", rp.metaAddr.String(), " builtin=", strconv.FormatUint(uint64(builtin), 16), " metaLocs=", itoa(len(metaLocs)), " userLocs=", itoa(len(userLocs)))

	if builtin&bePublicationAnnouncer != 0 {
		t.addBuiltinProxy(rp, entitySEDPPubWriter, entitySEDPPubReader)
	}
	if builtin&beSubscriptionAnnouncer != 0 {
		t.addBuiltinProxy(rp, entitySEDPSubWriter, entitySEDPSubReader)
	}
	if builtin&bePublicationDetector != 0 {
		t.addReaderProxy(t.sedpPub, makeGUID(prefix, entitySEDPPubReader), rp, rp.metaAddr, true)
	}
	if builtin&beSubscriptionDetector != 0 {
		t.addReaderProxy(t.sedpSub, makeGUID(prefix, entitySEDPSubReader), rp, rp.metaAddr, true)
	}
	t.announceTo(rp.metaAddr)
	rp.dirty = true
	t.discoveryDirty = true
}

func (t *Transport) addBuiltinProxy(rp *remoteParticipant, wid, ourReader entityID) {
	p := &writerProxy{
		guid: makeGUID(rp.prefix, wid), part: rp, builtin: true, readerEntity: ourReader,
		reliable: true, tl: true, readers: map[*reader]struct{}{}, missing: map[int64]struct{}{},
	}
	t.proxies[p.guid] = p
	// A pre-emptive ACKNACK makes the writer send a heartbeat right away.
	p.ackCount++
	t.mb.reset()
	t.mb.header(t.prefix)
	t.mb.infoDst(rp.prefix)
	t.mb.ackNack(ourReader, wid, 1, 0, nil, p.ackCount, false)
	t.sendTo(rp.metaAddr, t.mb.b)
}

// pickAddr chooses the unicast locator most likely to reach a participant:
// the address its announcement came from if listed, otherwise a
// non-loopback one unless the sender is on this host.
func pickAddr(locs []locator, src net.IP, fallback *net.UDPAddr) *net.UDPAddr {
	var first, nonLoop *net.UDPAddr
	for _, l := range locs {
		a := l.udpAddr()
		if a == nil {
			continue
		}
		if src != nil && a.IP.Equal(src) {
			return a
		}
		if first == nil {
			first = a
		}
		if nonLoop == nil && !a.IP.IsLoopback() {
			nonLoop = a
		}
	}
	if src != nil && src.IsLoopback() && first != nil {
		return first
	}
	if nonLoop != nil {
		return nonLoop
	}
	if first != nil {
		return first
	}
	return fallback
}

func (t *Transport) removeRemote(prefix guidPrefix) {
	rp := t.remotes[prefix]
	if rp == nil {
		return
	}
	for _, rw := range rp.writers {
		delete(t.proxies, rw.guid)
	}
	for _, rr := range rp.readers {
		for _, w := range t.writersByGUID {
			delete(w.proxies, rr.guid)
		}
	}
	delete(t.proxies, makeGUID(prefix, entitySEDPPubWriter))
	delete(t.proxies, makeGUID(prefix, entitySEDPSubWriter))
	delete(t.sedpPub.proxies, makeGUID(prefix, entitySEDPPubReader))
	delete(t.sedpSub.proxies, makeGUID(prefix, entitySEDPSubReader))
	delete(t.remotes, prefix)
	t.emit(&transport.Frame{Kind: transport.KindBye, From: rp.guid})
}

// --- SEDP ------------------------------------------------------------------

func (t *Transport) handleSEDP(p *writerProxy, iq *inlineQoS, payload []byte, key bool) {
	isPub := p.readerEntity == entitySEDPPubReader
	var ep remoteEndpoint
	var locs []locator
	ep.reliable = isPub // DDS defaults: writers reliable, readers best effort
	ep.depth = 1
	if payload != nil {
		ok := parsePLPayload(payload, func(q *param) {
			switch q.pid {
			case pidEndpointGUID:
				ep.guid = q.value.guid()
			case pidTopicName:
				ep.topic = q.value.str()
			case pidTypeName:
				ep.typ = q.value.str()
			case pidReliability:
				ep.reliable = q.value.u32() == reliabilityReliable
			case pidDurability:
				ep.tl = q.value.u32() >= durabilityTransient
			case pidHistory:
				ep.keepAll = q.value.u32() == historyKeepAll
				ep.depth = int(q.value.i32())
			case pidUnicastLocator:
				if l := q.value.locator(); l.kind == locatorUDPv4 {
					locs = append(locs, l)
				}
			}
		})
		if !ok {
			return
		}
	}
	if ep.guid.IsZero() {
		ep.guid = iq.keyHash
	}
	if ep.guid.IsZero() {
		return
	}
	if payload == nil || key || iq.disposed {
		t.removeRemoteEndpoint(ep.guid)
		return
	}
	rp := t.remotes[prefixOf(ep.guid)]
	if rp == nil || ep.topic == "" {
		return
	}
	ep.classify()
	if isPub {
		if _, ok := rp.writers[entityOf(ep.guid)]; ok {
			return
		}
		rw := &remoteWriter{remoteEndpoint: ep, part: rp}
		rp.writers[entityOf(ep.guid)] = rw
		t.debugf("remote writer ", ep.topic, " ", ep.typ)
		t.matchRemoteWriter(rw)
	} else {
		if _, ok := rp.readers[entityOf(ep.guid)]; ok {
			return
		}
		rr := &remoteReader{remoteEndpoint: ep, part: rp, addr: pickAddr(locs, rp.src, rp.userAddr)}
		rp.readers[entityOf(ep.guid)] = rr
		t.debugf("remote reader ", ep.topic, " ", ep.typ)
		for _, w := range t.writersByGUID {
			if w.kind != ekBuiltin && endpointsMatch(w.topic, w.typ, w.reliable, w.tl, rr.topic, rr.typ, rr.reliable, rr.tl) {
				t.addReaderProxy(w, rr.guid, rp, rr.addr, rr.reliable)
			}
		}
	}
	rp.dirty = true
	t.discoveryDirty = true
}

// classify derives the ROS view of a DDS endpoint from its topic name.
func (ep *remoteEndpoint) classify() {
	if ep.topic == discoveryInfoTopic {
		ep.kind = ekInfo
		return
	}
	if name, ok := rosTopic(ep.topic); ok {
		ep.kind, ep.rosName, ep.rosType = ekTopic, name, rosType(ep.typ)
		return
	}
	if name, req, ok := rosService(ep.topic); ok {
		if req {
			ep.kind = ekRequest
		} else {
			ep.kind = ekReply
		}
		ep.rosName, ep.rosType = name, rosServiceType(rosType(ep.typ))
		return
	}
	ep.kind = ekBuiltin // not a ROS endpoint; ignored
}

func (t *Transport) matchRemoteWriter(rw *remoteWriter) {
	p := t.proxies[rw.guid]
	if p == nil {
		p = &writerProxy{guid: rw.guid, part: rw.part, rw: rw, reliable: rw.reliable, tl: rw.tl, readers: map[*reader]struct{}{}, missing: map[int64]struct{}{}}
		t.proxies[rw.guid] = p
	}
	for _, r := range t.readersByGUID {
		if endpointsMatch(rw.topic, rw.typ, rw.reliable, rw.tl, r.topic, r.typ, r.reliable, r.tl) {
			t.matchReaderToProxy(r, p)
		}
	}
}

func (t *Transport) matchReaderToProxy(r *reader, p *writerProxy) {
	if p == nil {
		return
	}
	if _, ok := p.readers[r]; ok {
		return
	}
	p.readers[r] = struct{}{}
	if r.reliable && p.reliable {
		p.ackCount++
		t.mb.reset()
		t.mb.header(t.prefix)
		t.mb.infoDst(p.part.prefix)
		t.mb.ackNack(r.entity, entityOf(p.guid), 1, 0, nil, p.ackCount, false)
		t.sendTo(p.part.userAddr, t.mb.b)
	}
}

func (t *Transport) removeRemoteEndpoint(guid transport.GUID) {
	rp := t.remotes[prefixOf(guid)]
	if rp == nil {
		return
	}
	e := entityOf(guid)
	if rw, ok := rp.writers[e]; ok {
		delete(rp.writers, e)
		delete(t.proxies, rw.guid)
		t.debugf("remote writer ", rw.topic, " gone")
	}
	if rr, ok := rp.readers[e]; ok {
		delete(rp.readers, e)
		for _, w := range t.writersByGUID {
			delete(w.proxies, rr.guid)
		}
		t.debugf("remote reader ", rr.topic, " gone")
	}
	rp.dirty = true
	t.discoveryDirty = true
}

// --- user data -------------------------------------------------------------

func (t *Transport) deliverUser(p *writerProxy, iq *inlineQoS, payload []byte) {
	rw := p.rw
	if rw == nil || payload == nil || len(p.readers) == 0 {
		return
	}
	switch rw.kind {
	case ekTopic:
		t.emit(&transport.Frame{Kind: transport.KindData, From: p.part.guid, Topic: rw.rosName, Type: rw.rosType, Payload: payload})
	case ekRequest:
		t.inboundSeq++
		seq := t.inboundSeq
		t.inbound[seq] = &inboundRequest{writer: rw.guid, sn: p.expected - 1, related: iq.related, at: time.Now()}
		t.emit(&transport.Frame{Kind: transport.KindRequest, From: p.part.guid, Topic: rw.rosName, Type: rw.rosType, Seq: seq, Payload: payload})
	case ekReply:
		if !iq.haveRelated {
			return
		}
		w := t.writersByGUID[iq.related]
		if w == nil || w.kind != ekRequest {
			// Related identity names our reply reader; find the request
			// writer of this service holding that sequence number.
			w = nil
			for _, c := range t.writersByGUID {
				if c.kind == ekRequest && c.service == rw.rosName {
					if _, ok := c.pending[iq.relatedSN]; ok {
						w = c
						break
					}
				}
			}
			if w == nil {
				return
			}
		}
		pr, ok := w.pending[iq.relatedSN]
		if !ok {
			return
		}
		delete(w.pending, iq.relatedSN)
		t.emit(&transport.Frame{Kind: transport.KindResponse, From: p.part.guid, Topic: rw.rosName, Target: pr.owner, Seq: pr.seq, OK: true, Payload: payload})
	case ekInfo:
		if pe, ok := decodeParticipantEntities(payload); ok {
			p.part.entities = pe
			p.part.dirty = true
			t.discoveryDirty = true
		}
	}
}

// --- Hello synthesis -------------------------------------------------------

// emitHellos announces every remote participant to the goros side.
func (t *Transport) emitHellos(all bool) {
	for _, rp := range t.remotes {
		if all || rp.dirty {
			rp.dirty = false
			t.emit(&transport.Frame{Kind: transport.KindHello, From: rp.guid, Participant: t.participantInfo(rp)})
		}
	}
	t.discoveryDirty = false
}

func qosInfo(reliable, tl, keepAll bool, depth int) transport.QoSInfo {
	q := transport.QoSInfo{Reliability: 2, Durability: 2, History: 1, Depth: uint32(depth)}
	if reliable {
		q.Reliability = 1
	}
	if tl {
		q.Durability = 1
	}
	if keepAll {
		q.History = 2
	}
	return q
}

type helloEntry struct {
	guid transport.GUID
	dir  int // 0 pub, 1 sub, 2 service, 3 client
	ep   transport.EndpointInfo
}

// participantInfo renders a DDS participant as a goros participant: its
// ROS nodes (from ros_discovery_info) with their endpoints, plus a hidden
// catch-all node for endpoints not attributed to any node.
func (t *Transport) participantInfo(rp *remoteParticipant) *transport.ParticipantInfo {
	var entries []helloEntry
	services := map[string]*remoteReader{}
	replies := map[string]*remoteWriter{}
	replyReaders := map[string]*remoteReader{}
	for _, rw := range rp.writers {
		switch rw.kind {
		case ekTopic:
			entries = append(entries, helloEntry{rw.guid, 0, transport.EndpointInfo{Name: rw.rosName, Type: rw.rosType, QoS: qosInfo(rw.reliable, rw.tl, rw.keepAll, rw.depth)}})
		case ekReply:
			replies[rw.rosName] = rw
		}
	}
	for _, rr := range rp.readers {
		switch rr.kind {
		case ekTopic:
			entries = append(entries, helloEntry{rr.guid, 1, transport.EndpointInfo{Name: rr.rosName, Type: rr.rosType, QoS: qosInfo(rr.reliable, rr.tl, rr.keepAll, rr.depth)}})
		case ekRequest:
			services[rr.rosName] = rr
		case ekReply:
			replyReaders[rr.rosName] = rr
		}
	}
	for name, rr := range services {
		if _, ok := replies[name]; ok {
			entries = append(entries, helloEntry{rr.guid, 2, transport.EndpointInfo{Name: name, Type: rr.rosType}})
		}
	}
	for _, rw := range rp.writers {
		if rw.kind == ekRequest {
			if _, ok := replyReaders[rw.rosName]; ok {
				entries = append(entries, helloEntry{rw.guid, 3, transport.EndpointInfo{Name: rw.rosName, Type: rw.rosType}})
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].guid.String() < entries[j].guid.String() })

	info := &transport.ParticipantInfo{Domain: uint32(t.domain)}
	nodeOf := map[transport.GUID]int{}
	if rp.entities != nil {
		for i, ne := range rp.entities.nodes {
			info.Nodes = append(info.Nodes, transport.NodeInfo{Name: ne.name, Namespace: ne.namespace})
			for _, g := range ne.readers {
				nodeOf[g] = i
			}
			for _, g := range ne.writers {
				nodeOf[g] = i
			}
		}
	}
	other := transport.NodeInfo{Name: "_dds_" + rp.guid.String()[:8], Namespace: "/"}
	used := false
	for _, e := range entries {
		n := &other
		if i, ok := nodeOf[e.guid]; ok {
			n = &info.Nodes[i]
		} else {
			used = true
		}
		switch e.dir {
		case 0:
			n.Publishers = append(n.Publishers, e.ep)
		case 1:
			n.Subscriptions = append(n.Subscriptions, e.ep)
		case 2:
			n.Services = append(n.Services, e.ep)
		case 3:
			n.Clients = append(n.Clients, e.ep)
		}
	}
	if used {
		info.Nodes = append(info.Nodes, other)
	}
	return info
}

func (p guidPrefix) String() string {
	var g transport.GUID
	copy(g[:], p[:])
	return g.String()[:24]
}
