package goros

import (
	"sync"
	"time"
)

// Timer runs a callback periodically on the node's executor.
type Timer struct {
	node    *Node
	period  time.Duration
	cb      func()
	oneShot bool
	mu      sync.Mutex
	stop    chan struct{}
	done    bool
	gen     int
}

// NewTimer creates a wall timer that fires every period. The first firing
// happens one period after creation.
func (n *Node) NewTimer(period time.Duration, cb func()) *Timer {
	t := &Timer{node: n, period: period, cb: cb}
	n.mu.Lock()
	n.timers = append(n.timers, t)
	n.mu.Unlock()
	t.start()
	return t
}

// NewWallTimer is an alias of NewTimer matching rclcpp naming.
func (n *Node) NewWallTimer(period time.Duration, cb func()) *Timer { return n.NewTimer(period, cb) }

// NewOneShotTimer fires cb once after delay.
func (n *Node) NewOneShotTimer(delay time.Duration, cb func()) *Timer {
	t := &Timer{node: n, period: delay, cb: cb, oneShot: true}
	n.mu.Lock()
	n.timers = append(n.timers, t)
	n.mu.Unlock()
	t.start()
	return t
}

func (t *Timer) start() {
	t.mu.Lock()
	t.gen++
	gen := t.gen
	stop := make(chan struct{})
	t.stop = stop
	t.done = false
	t.mu.Unlock()
	go func() {
		ticker := time.NewTicker(t.period)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.node.ctx.Done():
				return
			case <-ticker.C:
				t.mu.Lock()
				live := !t.done && t.gen == gen
				t.mu.Unlock()
				if !live {
					return
				}
				if t.oneShot {
					t.Cancel()
				}
				t.node.enqueue(t.cb)
				if t.oneShot {
					return
				}
			}
		}
	}()
}

// Period returns the timer period.
func (t *Timer) Period() time.Duration { return t.period }

// Cancel stops the timer. It can be restarted with Reset.
func (t *Timer) Cancel() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return
	}
	t.done = true
	close(t.stop)
}

// IsCancelled reports whether the timer is stopped.
func (t *Timer) IsCancelled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.done
}

// Reset restarts the period from now.
func (t *Timer) Reset() {
	t.Cancel()
	t.start()
}
