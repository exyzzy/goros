package goros

import (
	"sync"
	"time"
)

// Executor runs node callbacks. Callbacks of all nodes added to a
// single-threaded executor run sequentially on one goroutine; a
// multi-threaded executor runs them on N goroutines.
type Executor struct {
	threads int
	queue   chan func()
	mu      sync.Mutex
	nodes   map[*Node]chan struct{}
	ctx     *Context
	cancel  chan struct{}
	running bool
}

// NewSingleThreadedExecutor creates an executor with one worker.
func NewSingleThreadedExecutor() *Executor { return newExecutor(1) }

// NewMultiThreadedExecutor creates an executor with n workers (0 = 4).
func NewMultiThreadedExecutor(n int) *Executor {
	if n <= 0 {
		n = 4
	}
	return newExecutor(n)
}

func newExecutor(threads int) *Executor {
	return &Executor{threads: threads, queue: make(chan func(), 1024), nodes: map[*Node]chan struct{}{}, cancel: make(chan struct{})}
}

// AddNode attaches a node. All nodes must belong to the same Context.
func (e *Executor) AddNode(n *Node) error {
	n.mu.Lock()
	if n.executor != nil {
		n.mu.Unlock()
		return ErrAlreadySpinning
	}
	n.executor = e
	n.mu.Unlock()

	e.mu.Lock()
	if e.ctx == nil {
		e.ctx = n.ctx
	}
	stop := make(chan struct{})
	e.nodes[n] = stop
	e.mu.Unlock()
	go func() {
		for {
			select {
			case <-stop:
				return
			case fn := <-n.queue:
				select {
				case e.queue <- fn:
				case <-stop:
					return
				}
			}
		}
	}()
	return nil
}

// RemoveNode detaches a node.
func (e *Executor) RemoveNode(n *Node) {
	e.mu.Lock()
	stop, ok := e.nodes[n]
	if ok {
		delete(e.nodes, n)
		close(stop)
	}
	e.mu.Unlock()
	n.mu.Lock()
	if n.executor == e {
		n.executor = nil
	}
	n.mu.Unlock()
}

// Spin runs callbacks until Cancel is called or the context shuts down.
func (e *Executor) Spin() {
	e.mu.Lock()
	ctx := e.ctx
	cancel := e.cancel
	e.running = true
	e.mu.Unlock()
	var done <-chan struct{}
	if ctx != nil {
		done = ctx.Done()
	}
	var wg sync.WaitGroup
	for i := 0; i < e.threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case fn := <-e.queue:
					fn()
				case <-cancel:
					return
				case <-done:
					return
				}
			}
		}()
	}
	wg.Wait()
	e.mu.Lock()
	e.running = false
	e.mu.Unlock()
}

// SpinOnce runs at most one callback, waiting up to timeout for one to
// become available (timeout <= 0 means do not wait). It returns whether a
// callback ran.
func (e *Executor) SpinOnce(timeout time.Duration) bool {
	if timeout <= 0 {
		select {
		case fn := <-e.queue:
			fn()
			return true
		default:
			return false
		}
	}
	e.mu.Lock()
	cancel := e.cancel
	e.mu.Unlock()
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case fn := <-e.queue:
		fn()
		return true
	case <-t.C:
		return false
	case <-cancel:
		return false
	}
}

// SpinSome runs all callbacks that are ready, for at most maxDuration
// (<= 0 means until the queue is empty).
func (e *Executor) SpinSome(maxDuration time.Duration) int {
	start := time.Now()
	n := 0
	for {
		if !e.SpinOnce(0) {
			return n
		}
		n++
		if maxDuration > 0 && time.Since(start) >= maxDuration {
			return n
		}
	}
}

// SpinUntil spins until cond is closed (e.g. a done channel) or the
// context shuts down.
func (e *Executor) SpinUntil(cond <-chan struct{}) {
	for {
		select {
		case <-cond:
			return
		default:
		}
		if e.ctx != nil && !e.ctx.OK() {
			return
		}
		e.SpinOnce(50 * time.Millisecond)
	}
}

// Cancel stops Spin. The executor can be spun again afterwards.
func (e *Executor) Cancel() {
	e.mu.Lock()
	defer e.mu.Unlock()
	select {
	case <-e.cancel:
	default:
		close(e.cancel)
	}
	e.cancel = make(chan struct{})
}

// Spin is the convenience used by most programs: it runs node on a
// single-threaded executor until the context shuts down.
func Spin(node *Node) error {
	ex := NewSingleThreadedExecutor()
	if err := ex.AddNode(node); err != nil {
		return err
	}
	defer ex.RemoveNode(node)
	ex.Spin()
	return nil
}

// SpinOnce runs one pending callback of node, waiting up to timeout.
func SpinOnce(node *Node, timeout time.Duration) bool {
	var deadline <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		deadline = t.C
	}
	select {
	case fn := <-node.queue:
		fn()
		return true
	case <-deadline:
		return false
	case <-node.ctx.Done():
		return false
	}
}

// SpinUntilFutureComplete spins node until done is closed or the timeout
// elapses (timeout <= 0 waits forever). It reports whether done completed.
func SpinUntilFutureComplete(node *Node, done <-chan struct{}, timeout time.Duration) bool {
	var deadline <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		deadline = t.C
	}
	for {
		select {
		case <-done:
			return true
		case <-deadline:
			return false
		case <-node.ctx.Done():
			return false
		case fn := <-node.queue:
			fn()
		}
	}
}
