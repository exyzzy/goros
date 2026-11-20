package rtps

import (
	"net"
	"sort"
	"strconv"
	"time"

	"bitbucket.org/goros/transport"
)

// This file holds the goros-facing half of the participant: the goros
// participants ("owners") announcing through Send, the RTPS writers and
// readers created for their endpoints, and the reliable-writer machinery.

type endpointKind uint8

const (
	ekTopic   endpointKind = iota // rt/ topic
	ekRequest                     // rq/ request topic
	ekReply                       // rr/ reply topic
	ekInfo                        // ros_discovery_info
	ekBuiltin                     // SPDP / SEDP
)

type writerKey struct {
	owner transport.GUID
	topic string
}

type readerKey struct {
	topic, typ   string
	reliable, tl bool
}

// owner is one goros participant announcing through this transport.
type owner struct {
	guid     transport.GUID
	info     transport.ParticipantInfo
	lastSeen time.Time
	writers  map[*writer]struct{}
	readers  map[*reader]struct{}
}

type sample struct {
	sn        int64
	payload   []byte
	inlineQoS []byte
	key       bool
}

type pendingRequest struct {
	owner transport.GUID
	seq   uint64
	at    time.Time
}

// readerProxy is a matched remote reader as seen by one local writer.
type readerProxy struct {
	guid     transport.GUID
	prefix   guidPrefix
	entity   entityID
	addr     *net.UDPAddr
	reliable bool
	startSN  int64 // first sample relevant to this reader
	acked    int64 // everything <= acked was acknowledged
	ackCount int32
	hasAcked bool
}

type writer struct {
	guid     transport.GUID
	entity   entityID
	topic    string
	typ      string
	reliable bool
	tl       bool
	keepAll  bool
	depth    int
	kind     endpointKind
	service  string
	owner    *owner
	hist     []*sample
	lastSN   int64
	hbCount  int32
	proxies  map[transport.GUID]*readerProxy
	pending  map[int64]pendingRequest
}

type reader struct {
	guid     transport.GUID
	entity   entityID
	topic    string
	typ      string
	reliable bool
	tl       bool
	depth    int
	kind     endpointKind
	service  string
	owners   map[*owner]struct{}
}

type inboundRequest struct {
	writer  transport.GUID // the client's request writer
	sn      int64
	related transport.GUID // the client's reply reader, when it told us
	at      time.Time
}

func (w *writer) histFirst() int64 {
	if len(w.hist) == 0 {
		return w.lastSN + 1
	}
	return w.hist[0].sn
}

func (w *writer) find(sn int64) *sample {
	i := sort.Search(len(w.hist), func(i int) bool { return w.hist[i].sn >= sn })
	if i < len(w.hist) && w.hist[i].sn == sn {
		return w.hist[i]
	}
	return nil
}

// --- builtin endpoints -----------------------------------------------------

func (t *Transport) initBuiltin() {
	t.sedpPub = &writer{entity: entitySEDPPubWriter, reliable: true, tl: true, keepAll: true, kind: ekBuiltin, proxies: map[transport.GUID]*readerProxy{}}
	t.sedpPub.guid = makeGUID(t.prefix, t.sedpPub.entity)
	t.sedpSub = &writer{entity: entitySEDPSubWriter, reliable: true, tl: true, keepAll: true, kind: ekBuiltin, proxies: map[transport.GUID]*readerProxy{}}
	t.sedpSub.guid = makeGUID(t.prefix, t.sedpSub.entity)
	t.writersByGUID[t.sedpPub.guid] = t.sedpPub
	t.writersByGUID[t.sedpSub.guid] = t.sedpSub

	t.infoWriter = t.newWriter(nil, discoveryInfoTopic, discoveryInfoType, true, true, false, 1, ekInfo, "")
	t.infoReader = t.newReader(discoveryInfoTopic, discoveryInfoType, true, true, 1, ekInfo, "")
	t.publishDiscoveryInfo()
}

// --- owners ----------------------------------------------------------------

type wantWriter struct {
	topic, typ   string
	reliable, tl bool
	keepAll      bool
	depth        int
	kind         endpointKind
	service      string
}

type wantReader struct {
	key     readerKey
	depth   int
	kind    endpointKind
	service string
}

func qosFlags(q transport.QoSInfo) (reliable, tl, keepAll bool, depth int) {
	reliable = q.Reliability != 2
	tl = q.Durability == 1
	keepAll = q.History == 2
	depth = int(q.Depth)
	if depth <= 0 {
		depth = 10
	}
	return
}

// applyHello reconciles the endpoints of one goros participant.
func (t *Transport) applyHello(from transport.GUID, info *transport.ParticipantInfo) {
	o := t.owners[from]
	if o == nil {
		o = &owner{guid: from, writers: map[*writer]struct{}{}, readers: map[*reader]struct{}{}}
		t.owners[from] = o
		t.debugf("goros participant ", from.String(), " joined")
	}
	o.lastSeen = time.Now()
	o.info = *info

	wantW := map[string]wantWriter{}
	wantR := map[readerKey]wantReader{}
	addW := func(w wantWriter) {
		if _, ok := wantW[w.topic]; !ok {
			wantW[w.topic] = w
		}
	}
	addR := func(r wantReader) {
		if _, ok := wantR[r.key]; !ok {
			wantR[r.key] = r
		}
	}
	for ni := range info.Nodes {
		n := &info.Nodes[ni]
		for _, e := range n.Publishers {
			rel, tl, ka, depth := qosFlags(e.QoS)
			addW(wantWriter{ddsTopic(e.Name), ddsType(e.Type), rel, tl, ka, depth, ekTopic, ""})
		}
		for _, e := range n.Subscriptions {
			rel, tl, _, depth := qosFlags(e.QoS)
			addR(wantReader{readerKey{ddsTopic(e.Name), ddsType(e.Type), rel, tl}, depth, ekTopic, ""})
		}
		for _, e := range n.Services {
			addR(wantReader{readerKey{ddsRequestTopic(e.Name), ddsRequestType(e.Type), true, false}, 10, ekRequest, e.Name})
			addW(wantWriter{ddsReplyTopic(e.Name), ddsResponseType(e.Type), true, false, false, 10, ekReply, e.Name})
		}
		for _, e := range n.Clients {
			addW(wantWriter{ddsRequestTopic(e.Name), ddsRequestType(e.Type), true, false, false, 10, ekRequest, e.Name})
			addR(wantReader{readerKey{ddsReplyTopic(e.Name), ddsResponseType(e.Type), true, false}, 10, ekReply, e.Name})
		}
	}
	changed := false
	for topic, spec := range wantW {
		if w := t.writers[writerKey{from, topic}]; w == nil {
			w = t.newWriter(o, spec.topic, spec.typ, spec.reliable, spec.tl, spec.keepAll, spec.depth, spec.kind, spec.service)
			o.writers[w] = struct{}{}
			changed = true
		}
	}
	for key, spec := range wantR {
		r := t.readers[key]
		if r == nil {
			r = t.newReader(key.topic, key.typ, key.reliable, key.tl, spec.depth, spec.kind, spec.service)
			changed = true
		}
		if _, ok := r.owners[o]; !ok {
			r.owners[o] = struct{}{}
			o.readers[r] = struct{}{}
			changed = true
		}
	}
	for w := range o.writers {
		if _, ok := wantW[w.topic]; !ok {
			t.removeWriter(w)
			delete(o.writers, w)
			changed = true
		}
	}
	for r := range o.readers {
		key := readerKey{r.topic, r.typ, r.reliable, r.tl}
		if _, ok := wantR[key]; !ok {
			delete(r.owners, o)
			delete(o.readers, r)
			if len(r.owners) == 0 {
				t.removeReader(r)
			}
			changed = true
		}
	}
	if changed {
		t.publishDiscoveryInfo()
	}
}

func (t *Transport) removeOwner(g transport.GUID) {
	o := t.owners[g]
	if o == nil {
		return
	}
	for w := range o.writers {
		t.removeWriter(w)
	}
	for r := range o.readers {
		delete(r.owners, o)
		if len(r.owners) == 0 {
			t.removeReader(r)
		}
	}
	delete(t.owners, g)
	t.debugf("goros participant ", g.String(), " left")
	t.publishDiscoveryInfo()
}

// --- local endpoints -------------------------------------------------------

func (t *Transport) newWriter(o *owner, topic, typ string, reliable, tl, keepAll bool, depth int, kind endpointKind, service string) *writer {
	w := &writer{
		entity: t.newEntity(0x03), topic: topic, typ: typ, reliable: reliable, tl: tl, keepAll: keepAll, depth: depth,
		kind: kind, service: service, owner: o,
		proxies: map[transport.GUID]*readerProxy{}, pending: map[int64]pendingRequest{},
	}
	w.guid = makeGUID(t.prefix, w.entity)
	var og transport.GUID
	if o != nil {
		og = o.guid
	}
	t.writers[writerKey{og, topic}] = w
	t.writersByGUID[w.guid] = w
	t.write(t.sedpPub, t.endpointData(w.guid, topic, typ, reliable, tl, keepAll, depth), nil, nil)
	for _, rp := range t.remotes {
		for _, rr := range rp.readers {
			if endpointsMatch(topic, typ, reliable, tl, rr.topic, rr.typ, rr.reliable, rr.tl) {
				t.addReaderProxy(w, rr.guid, rp, rr.addr, rr.reliable)
			}
		}
	}
	t.debugf("writer ", topic, " ", w.guid.String())
	return w
}

func (t *Transport) removeWriter(w *writer) {
	var og transport.GUID
	if w.owner != nil {
		og = w.owner.guid
	}
	delete(t.writers, writerKey{og, w.topic})
	delete(t.writersByGUID, w.guid)
	t.write(t.sedpPub, nil, disposeQoS(w.guid), nil)
	t.debugf("writer ", w.topic, " removed")
}

func (t *Transport) newReader(topic, typ string, reliable, tl bool, depth int, kind endpointKind, service string) *reader {
	r := &reader{
		entity: t.newEntity(0x04), topic: topic, typ: typ, reliable: reliable, tl: tl, depth: depth,
		kind: kind, service: service, owners: map[*owner]struct{}{},
	}
	r.guid = makeGUID(t.prefix, r.entity)
	t.readers[readerKey{topic, typ, reliable, tl}] = r
	t.readersByGUID[r.guid] = r
	t.write(t.sedpSub, t.endpointData(r.guid, topic, typ, reliable, tl, false, depth), nil, nil)
	for _, rp := range t.remotes {
		for _, rw := range rp.writers {
			if endpointsMatch(rw.topic, rw.typ, rw.reliable, rw.tl, topic, typ, reliable, tl) {
				t.matchReaderToProxy(r, t.proxies[rw.guid])
			}
		}
	}
	t.debugf("reader ", topic, " ", r.guid.String())
	return r
}

func (t *Transport) removeReader(r *reader) {
	delete(t.readers, readerKey{r.topic, r.typ, r.reliable, r.tl})
	delete(t.readersByGUID, r.guid)
	for _, p := range t.proxies {
		delete(p.readers, r)
	}
	t.write(t.sedpSub, nil, disposeQoS(r.guid), nil)
	t.debugf("reader ", r.topic, " removed")
}

// endpointsMatch applies the DDS request/offer rules goros honours.
func endpointsMatch(wTopic, wType string, wRel, wTL bool, rTopic, rType string, rRel, rTL bool) bool {
	return wTopic == rTopic && wType == rType && (!rRel || wRel) && (!rTL || wTL)
}

// addReaderProxy matches a remote reader to a local writer.
func (t *Transport) addReaderProxy(w *writer, guid transport.GUID, rp *remoteParticipant, addr *net.UDPAddr, reliable bool) {
	if _, ok := w.proxies[guid]; ok {
		return
	}
	p := &readerProxy{guid: guid, prefix: prefixOf(guid), entity: entityOf(guid), addr: addr, reliable: reliable}
	if w.tl {
		p.startSN = w.histFirst()
	} else {
		p.startSN = w.lastSN + 1
	}
	w.proxies[guid] = p
	if w.tl {
		for _, s := range w.hist {
			if s.sn >= p.startSN {
				t.sendSample(w, s, p, w.reliable)
			}
		}
	}
	if w.reliable && reliable {
		t.sendHeartbeat(w, p)
	}
}

// --- writing ---------------------------------------------------------------

// write appends a sample to w's history and sends it to every matched
// reader (or just to only). It returns the sequence number.
func (t *Transport) write(w *writer, payload, inlineQoS []byte, only *readerProxy) int64 {
	w.lastSN++
	s := &sample{sn: w.lastSN, payload: payload, inlineQoS: inlineQoS, key: false}
	w.hist = append(w.hist, s)
	if !w.keepAll {
		max := w.depth
		if !w.tl && max < 16 {
			max = 16
		}
		if len(w.hist) > max {
			copy(w.hist, w.hist[len(w.hist)-max:])
			w.hist = w.hist[:max]
		}
	}
	if only != nil {
		t.sendSample(w, s, only, w.reliable)
		return s.sn
	}
	// One copy per remote participant: readers of the same participant
	// share an address, so address the DATA to all readers (unknown id).
	sent := map[guidPrefix]*readerProxy{}
	for _, p := range w.proxies {
		if prev, ok := sent[p.prefix]; ok {
			if prev != nil {
				t.sendSampleAs(w, s, prev, entityUnknown, w.reliable)
				sent[p.prefix] = nil
			}
			continue
		}
		sent[p.prefix] = p
	}
	for _, p := range sent {
		if p != nil {
			t.sendSample(w, s, p, w.reliable)
		}
	}
	return s.sn
}

func (t *Transport) sendSample(w *writer, s *sample, p *readerProxy, heartbeat bool) {
	t.sendSampleAs(w, s, p, p.entity, heartbeat)
}

// sendSampleAs transmits one sample to p, addressed to reader entity rid.
func (t *Transport) sendSampleAs(w *writer, s *sample, p *readerProxy, rid entityID, heartbeat bool) {
	now := timeFromNanos(time.Now().UnixNano())
	first := w.histFirst()
	if p.startSN > first {
		first = p.startSN
	}
	if len(s.payload)+len(s.inlineQoS)+128 <= maxDatagram {
		t.mb.reset()
		t.mb.header(t.prefix)
		t.mb.infoDst(p.prefix)
		t.mb.infoTS(now)
		t.mb.data(rid, w.entity, s.sn, s.inlineQoS, s.payload, s.key)
		if heartbeat {
			w.hbCount++
			t.mb.heartbeat(rid, w.entity, first, w.lastSN, w.hbCount, false)
		}
		t.sendTo(p.addr, t.mb.b)
		return
	}
	total := (len(s.payload) + fragSize - 1) / fragSize
	for i := 0; i < total; i++ {
		chunk := s.payload[i*fragSize:]
		if len(chunk) > fragSize {
			chunk = chunk[:fragSize]
		}
		t.mb.reset()
		t.mb.header(t.prefix)
		t.mb.infoDst(p.prefix)
		t.mb.infoTS(now)
		var qos []byte
		if i == 0 {
			qos = s.inlineQoS
		}
		t.mb.dataFrag(rid, w.entity, s.sn, uint32(i+1), 1, uint16(fragSize), uint32(len(s.payload)), qos, chunk)
		t.sendTo(p.addr, t.mb.b)
	}
	if heartbeat {
		t.mb.reset()
		t.mb.header(t.prefix)
		t.mb.infoDst(p.prefix)
		w.hbCount++
		t.mb.heartbeat(rid, w.entity, first, w.lastSN, w.hbCount, false)
		t.sendTo(p.addr, t.mb.b)
	}
}

func (t *Transport) sendHeartbeat(w *writer, p *readerProxy) {
	first := w.histFirst()
	if p.startSN > first {
		first = p.startSN
	}
	w.hbCount++
	t.mb.reset()
	t.mb.header(t.prefix)
	t.mb.infoDst(p.prefix)
	t.mb.heartbeat(p.entity, w.entity, first, w.lastSN, w.hbCount, false)
	t.sendTo(p.addr, t.mb.b)
}

// heartbeatAll nudges every reliable reader that has not acknowledged the
// latest sample.
func (t *Transport) heartbeatAll(w *writer) {
	if !w.reliable {
		return
	}
	for _, p := range w.proxies {
		if p.reliable && p.acked < w.lastSN {
			t.sendHeartbeat(w, p)
		}
	}
}

// onAckNack serves retransmission requests from a reader.
func (t *Transport) onAckNack(w *writer, p *readerProxy, base int64, numBits uint32, bitmap []uint32, count int32, final bool) {
	if p.hasAcked && count == p.ackCount {
		return
	}
	p.hasAcked, p.ackCount = true, count
	if base-1 > p.acked {
		p.acked = base - 1
	}
	first := w.histFirst()
	if p.startSN > first {
		first = p.startSN
	}
	var resend []*sample
	var gaps []int64
	for i := uint32(0); i < numBits && i < uint32(len(bitmap))*32; i++ {
		if bitmap[i/32]&(1<<(31-i%32)) == 0 {
			continue
		}
		sn := base + int64(i)
		if sn > w.lastSN {
			break
		}
		if sn < first {
			gaps = append(gaps, sn)
			continue
		}
		if s := w.find(sn); s != nil {
			resend = append(resend, s)
		} else {
			gaps = append(gaps, sn)
		}
	}
	if len(gaps) > 0 {
		t.mb.reset()
		t.mb.header(t.prefix)
		t.mb.infoDst(p.prefix)
		start, end := gaps[0], gaps[0]
		for _, sn := range gaps[1:] {
			if sn == end+1 {
				end = sn
				continue
			}
			t.mb.gap(p.entity, w.entity, start, end)
			start, end = sn, sn
		}
		t.mb.gap(p.entity, w.entity, start, end)
		t.sendTo(p.addr, t.mb.b)
	}
	for i, s := range resend {
		t.sendSample(w, s, p, i == len(resend)-1)
	}
	// A reader that has not acknowledged everything gets a heartbeat even
	// when it set the final flag: Cyclone DDS sends its pre-emptive
	// ACKNACK with the flag set and still expects to learn our history.
	if len(resend) == 0 && base <= w.lastSN {
		t.sendHeartbeat(w, p)
	}
}

// --- discovery payloads ----------------------------------------------------

func (t *Transport) endpointData(guid transport.GUID, topic, typ string, reliable, tl, keepAll bool, depth int) []byte {
	var pl plist
	pl.guidParam(pidEndpointGUID, guid)
	pl.guidParam(pidParticipantGUID, t.guid)
	pl.strParam(pidTopicName, topic)
	pl.strParam(pidTypeName, typ)
	pl.reliability(reliable)
	pl.durability(tl)
	pl.history(keepAll, int32(depth))
	pl.param(pidLiveliness, func() {
		pl.u32(0) // automatic
		pl.time(timeInfinite)
	})
	pl.param(pidDataRepresentation, func() {
		pl.u32(1)
		pl.u16(0) // XCDR1
	})
	pl.param(pidProtocolVersion, func() { pl.u8(protocolMajor); pl.u8(protocolMinor) })
	pl.param(pidVendorID, func() { pl.raw(vendorID[:]) })
	for _, l := range t.locUni {
		pl.locatorParam(pidUnicastLocator, l)
	}
	pl.sentinel()
	return pl.encapsulated()
}

// disposeQoS is the inline QoS of a dispose/unregister for key guid.
func disposeQoS(guid transport.GUID) []byte {
	var pl plist
	pl.guidParam(pidKeyHash, guid)
	pl.param(pidStatusInfo, func() { pl.raw([]byte{0, 0, 0, 3}) })
	pl.sentinel()
	return pl.bytes()
}

func (t *Transport) spdpPayload() []byte {
	var pl plist
	pl.param(pidProtocolVersion, func() { pl.u8(protocolMajor); pl.u8(protocolMinor) })
	pl.param(pidVendorID, func() { pl.raw(vendorID[:]) })
	pl.guidParam(pidParticipantGUID, t.guid)
	pl.u32Param(pidBuiltinEndpointSet, beParticipantAnnouncer|beParticipantDetector|bePublicationAnnouncer|bePublicationDetector|beSubscriptionAnnouncer|beSubscriptionDetector)
	pl.u32Param(pidDomainID, uint32(t.domain))
	pl.param(pidParticipantLeaseDuration, func() { pl.time(timeFromNanos(int64(t.opts.Lease))) })
	for _, l := range t.locUni {
		pl.locatorParam(pidMetatrafficUnicastLocator, l)
		pl.locatorParam(pidDefaultUnicastLocator, l)
	}
	// No metatraffic multicast locator: peers then unicast discovery data
	// to us, which works on every network (multicast loopback is not a
	// given on macOS, and Wi-Fi often drops multicast).
	pl.strParam(pidEntityName, t.opts.Name)
	pl.param(pidUserData, func() {
		const ud = "enclave=/;"
		pl.u32(uint32(len(ud)))
		pl.raw([]byte(ud))
	})
	pl.sentinel()
	return pl.encapsulated()
}

// announce sends our SPDP sample to the multicast group, the static peers
// and every known participant.
func (t *Transport) announce() {
	t.spdpSN++
	t.mb.reset()
	t.mb.header(t.prefix)
	t.mb.infoTS(timeFromNanos(time.Now().UnixNano()))
	t.mb.data(entitySPDPReader, entitySPDPWriter, t.spdpSN, nil, t.spdpPayload(), false)
	t.broadcastSPDP(t.mb.b)
}

func (t *Transport) announceDispose() {
	t.spdpSN++
	t.mb.reset()
	t.mb.header(t.prefix)
	t.mb.data(entitySPDPReader, entitySPDPWriter, t.spdpSN, disposeQoS(t.guid), nil, false)
	t.broadcastSPDP(t.mb.b)
}

func (t *Transport) broadcastSPDP(b []byte) {
	if t.multiAddr != nil {
		t.sendTo(t.multiAddr, b)
	}
	for _, a := range t.peers {
		t.sendTo(a, b)
	}
	for _, rp := range t.remotes {
		t.sendTo(rp.metaAddr, b)
	}
}

// announceTo sends our SPDP sample to one participant.
func (t *Transport) announceTo(addr *net.UDPAddr) {
	t.spdpSN++
	t.mb.reset()
	t.mb.header(t.prefix)
	t.mb.infoTS(timeFromNanos(time.Now().UnixNano()))
	t.mb.data(entitySPDPReader, entitySPDPWriter, t.spdpSN, nil, t.spdpPayload(), false)
	t.sendTo(addr, t.mb.b)
}

// publishDiscoveryInfo republishes which nodes own which endpoints.
func (t *Transport) publishDiscoveryInfo() {
	if t.infoWriter == nil {
		return
	}
	pe := &participantEntities{gid: t.guid}
	owners := make([]*owner, 0, len(t.owners))
	for _, o := range t.owners {
		owners = append(owners, o)
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i].guid.String() < owners[j].guid.String() })
	for _, o := range owners {
		for ni := range o.info.Nodes {
			n := &o.info.Nodes[ni]
			ne := nodeEntities{namespace: n.Namespace, name: n.Name}
			addW := func(topic string) {
				if w := t.writers[writerKey{o.guid, topic}]; w != nil {
					ne.writers = append(ne.writers, w.guid)
				}
			}
			addR := func(key readerKey) {
				if r := t.readers[key]; r != nil {
					ne.readers = append(ne.readers, r.guid)
				}
			}
			for _, e := range n.Publishers {
				addW(ddsTopic(e.Name))
			}
			for _, e := range n.Subscriptions {
				rel, tl, _, _ := qosFlags(e.QoS)
				addR(readerKey{ddsTopic(e.Name), ddsType(e.Type), rel, tl})
			}
			for _, e := range n.Services {
				addR(readerKey{ddsRequestTopic(e.Name), ddsRequestType(e.Type), true, false})
				addW(ddsReplyTopic(e.Name))
			}
			for _, e := range n.Clients {
				addW(ddsRequestTopic(e.Name))
				addR(readerKey{ddsReplyTopic(e.Name), ddsResponseType(e.Type), true, false})
			}
			pe.nodes = append(pe.nodes, ne)
		}
	}
	t.write(t.infoWriter, encodeParticipantEntities(t.enc, pe), nil, nil)
}

// sendResponse answers a request that came in from a DDS client.
func (t *Transport) sendResponse(f *transport.Frame) {
	in := t.inbound[f.Seq]
	if in == nil {
		return
	}
	delete(t.inbound, f.Seq)
	if !f.OK {
		t.debugf("service ", f.Topic, " failed for DDS client: ", f.Error)
		return
	}
	w := t.writers[writerKey{f.From, ddsReplyTopic(f.Topic)}]
	if w == nil {
		return
	}
	target := in.related
	if target.IsZero() {
		target = in.writer
	}
	var pl plist
	pl.relatedSampleIdentity(target, in.sn)
	pl.sentinel()
	var only *readerProxy
	if !in.related.IsZero() {
		only = w.proxies[in.related]
	}
	t.write(w, f.Payload, pl.bytes(), only)
}

func itoa(n int) string { return strconv.Itoa(n) }
