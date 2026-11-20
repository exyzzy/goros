//go:build tinygo

package udp

import "errors"

// New opens a unicast UDP transport (TinyGo has no multicast). opts.Peers
// must list the other participants, typically the host running
// `goros bridge --udp-listen :7405`.
func New(opts Options) (*Transport, error) {
	if len(opts.Peers) == 0 {
		return nil, errors.New("udp: TinyGo transport needs at least one peer")
	}
	listen := opts.Listen
	if listen == "" {
		listen = ":" + portString(opts.Domain)
	}
	return NewUnicast(listen, opts.Peers)
}
