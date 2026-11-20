package goros

import (
	"crypto/rand"
	"time"

	"bitbucket.org/goros/transport"
)

func newGUID() transport.GUID {
	var g transport.GUID
	if _, err := rand.Read(g[:]); err == nil && !g.IsZero() {
		return g
	}
	// Fallback for targets without a hardware RNG: xorshift seeded from the
	// clock. Uniqueness across participants is then best effort.
	x := uint64(time.Now().UnixNano()) | 1
	for i := 0; i < len(g); i += 8 {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		for j := 0; j < 8 && i+j < len(g); j++ {
			g[i+j] = byte(x >> (8 * j))
		}
	}
	return g
}
