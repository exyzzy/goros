//go:build tinygo

package rtps

import (
	"errors"
	"net"
	"strconv"
)

// openSockets under TinyGo binds one unicast socket; there is no multicast,
// so Options.Peers must name the hosts to announce to (ROS_STATIC_PEERS on
// the ROS 2 side does the reverse) and Options.Advertise must carry the
// board's own address.
func (t *Transport) openSockets() error {
	if len(t.opts.Peers) == 0 {
		return errors.New("rtps: TinyGo needs Options.Peers")
	}
	if len(t.opts.Advertise) == 0 {
		return errors.New("rtps: TinyGo needs Options.Advertise")
	}
	pid := t.opts.ParticipantID
	if pid < 0 {
		pid = 0
	}
	listen := t.opts.Listen
	if listen == "" {
		listen = ":" + strconv.Itoa(MetatrafficUnicastPort(t.domain, pid))
	}
	laddr, err := net.ResolveUDPAddr("udp4", listen)
	if err != nil {
		return err
	}
	t.uni, err = net.ListenUDP("udp4", laddr)
	if err != nil {
		return err
	}
	t.uniPort = laddr.Port
	for _, ip := range t.opts.Advertise {
		t.locUni = append(t.locUni, udpv4Locator(ip, t.uniPort))
	}
	return nil
}
