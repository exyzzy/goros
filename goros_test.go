package goros

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/goros/msgs/example_interfaces"
	"bitbucket.org/goros/msgs/geometry_msgs"
	"bitbucket.org/goros/msgs/rcl_interfaces"
	"bitbucket.org/goros/msgs/sensor_msgs"
	"bitbucket.org/goros/msgs/std_msgs"
	"bitbucket.org/goros/msgs/std_srvs"
	"bitbucket.org/goros/transport"
	"bitbucket.org/goros/transport/inproc"
	"bitbucket.org/goros/transport/serial"
	"bitbucket.org/goros/transport/udp"
)

func quiet(opts ...Option) []Option {
	return append([]Option{WithLogWriter(io.Discard), WithoutDefaultTransport(), WithHelloPeriod(50 * time.Millisecond)}, opts...)
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestIntraProcessPubSub(t *testing.T) {
	ctx, err := Init(quiet()...)
	if err != nil {
		t.Fatal(err)
	}
	defer ctx.Shutdown()
	node, err := ctx.NewNode("test", WithNamespace("/ns"))
	if err != nil {
		t.Fatal(err)
	}
	pub, err := NewPublisher[*std_msgs.String](node, "chatter", QoSDefault)
	if err != nil {
		t.Fatal(err)
	}
	if pub.Topic() != "/ns/chatter" {
		t.Fatal(pub.Topic())
	}
	got := make(chan string, 10)
	sub, err := NewSubscription(node, "chatter", QoSDefault, func(m *std_msgs.String) { got <- m.Data })
	if err != nil {
		t.Fatal(err)
	}
	if sub.PublisherCount() != 1 || pub.SubscriptionCount() != 1 {
		t.Fatal("counts")
	}
	ex := NewSingleThreadedExecutor()
	ex.AddNode(node)
	go ex.Spin()
	defer ex.Cancel()

	pub.Publish(&std_msgs.String{Data: "hello"})
	pub.Publish(&std_msgs.String{Data: "world"})
	select {
	case s := <-got:
		if s != "hello" {
			t.Fatal(s)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
	if s := <-got; s != "world" {
		t.Fatal(s)
	}

	// Type mismatch on the same topic is rejected.
	if _, err := NewPublisher[*std_msgs.Int32](node, "chatter", QoSDefault); err != ErrTypeMismatch {
		t.Fatal(err)
	}
	// Incompatible QoS: best-effort publisher does not reach reliable sub.
	be, _ := NewPublisher[*std_msgs.String](node, "chatter", QoSSensorData)
	be.Publish(&std_msgs.String{Data: "lost"})
	select {
	case s := <-got:
		t.Fatal("unexpected", s)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestKeepLastDepth(t *testing.T) {
	ctx, _ := Init(quiet()...)
	defer ctx.Shutdown()
	node, _ := ctx.NewNode("depth")
	pub, _ := NewPublisher[*std_msgs.Int32](node, "n", QoSDefault)
	var got []int32
	sub, _ := NewSubscription(node, "n", KeepLastQoS(3), func(m *std_msgs.Int32) { got = append(got, m.Data) })
	for i := int32(0); i < 10; i++ {
		pub.Publish(&std_msgs.Int32{Data: i})
	}
	// Nothing is processed until we spin; only the last 3 survive.
	for SpinOnce(node, 10*time.Millisecond) {
	}
	if len(got) != 3 || got[0] != 7 || got[2] != 9 {
		t.Fatal(got)
	}
	if sub.Dropped() != 7 {
		t.Fatal(sub.Dropped())
	}
}

func twoContexts(t *testing.T) (*Context, *Context) {
	t.Helper()
	bus := inproc.NewBus(256)
	a, err := Init(quiet(WithTransport(bus.Join()))...)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Init(quiet(WithTransport(bus.Join()))...)
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

func TestDiscoveryAndRemotePubSub(t *testing.T) {
	a, b := twoContexts(t)
	defer a.Shutdown()
	defer b.Shutdown()
	talker, _ := a.NewNode("talker")
	listener, _ := b.NewNode("listener", WithNamespace("/robot"))
	pub, _ := NewPublisher[*geometry_msgs.Twist](talker, "/cmd_vel", QoSDefault)
	got := make(chan *geometry_msgs.Twist, 10)
	NewSubscription(listener, "/cmd_vel", QoSDefault, func(m *geometry_msgs.Twist) { got <- m })

	waitFor(t, 2*time.Second, func() bool { return pub.SubscriptionCount() == 1 })
	names := talker.GetNodeNames()
	if len(names) != 2 || names[0] != "/robot/listener" || names[1] != "/talker" {
		t.Fatal(names)
	}
	topics := listener.GetTopicNamesAndTypes()
	if tt := topics["/cmd_vel"]; len(tt) != 1 || tt[0] != "geometry_msgs/msg/Twist" {
		t.Fatal(topics)
	}

	go Spin(listener)
	pub.Publish(&geometry_msgs.Twist{Linear: geometry_msgs.Vector3{X: 1.5}, Angular: geometry_msgs.Vector3{Z: -0.25}})
	select {
	case m := <-got:
		if m.Linear.X != 1.5 || m.Angular.Z != -0.25 {
			t.Fatal(m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	// Participant departure is noticed.
	b.Shutdown()
	waitFor(t, 2*time.Second, func() bool { return len(talker.GetNodeNames()) == 1 })
}

func TestTransientLocalReplay(t *testing.T) {
	a, b := twoContexts(t)
	defer a.Shutdown()
	defer b.Shutdown()
	na, _ := a.NewNode("a")
	qos := QoSProfile{KeepLast, 2, Reliable, TransientLocal}
	pub, _ := NewPublisher[*std_msgs.Int32](na, "latched", qos)
	for i := int32(1); i <= 3; i++ {
		pub.Publish(&std_msgs.Int32{Data: i})
	}
	// Local late joiner.
	var local []int32
	NewSubscription(na, "latched", qos, func(m *std_msgs.Int32) { local = append(local, m.Data) })
	for SpinOnce(na, 10*time.Millisecond) {
	}
	if len(local) != 2 || local[0] != 2 || local[1] != 3 {
		t.Fatal(local)
	}
	// Remote late joiner.
	nb, _ := b.NewNode("b")
	got := make(chan int32, 10)
	NewSubscription(nb, "latched", qos, func(m *std_msgs.Int32) { got <- m.Data })
	go Spin(nb)
	var remote []int32
	for len(remote) < 2 {
		select {
		case v := <-got:
			remote = append(remote, v)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout", remote)
		}
	}
	if remote[0] != 2 || remote[1] != 3 {
		t.Fatal(remote)
	}
	// Volatile subscription must not match a transient-local publisher? It
	// does (TL offered >= Volatile requested), but gets no replay.
}

func TestServices(t *testing.T) {
	a, b := twoContexts(t)
	defer a.Shutdown()
	defer b.Shutdown()
	server, _ := a.NewNode("server")
	_, err := NewService(server, "add_two_ints", func(req *example_interfaces.AddTwoInts_Request, resp *example_interfaces.AddTwoInts_Response) error {
		resp.Sum = req.A + req.B
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	go Spin(server)

	// Local call (same context, different node).
	localNode, _ := a.NewNode("local_client")
	lc, _ := NewClient[example_interfaces.AddTwoInts_Request, example_interfaces.AddTwoInts_Response](localNode, "add_two_ints")
	if !lc.WaitForService(time.Second) {
		t.Fatal("local service not ready")
	}
	resp, err := lc.Call(context.Background(), &example_interfaces.AddTwoInts_Request{A: 2, B: 3})
	if err != nil || resp.Sum != 5 {
		t.Fatal(resp, err)
	}

	// Remote call.
	client, _ := b.NewNode("client")
	cli, _ := NewClient[example_interfaces.AddTwoInts_Request, example_interfaces.AddTwoInts_Response](client, "add_two_ints")
	if !cli.WaitForService(2 * time.Second) {
		t.Fatal("remote service not ready")
	}
	resp, err = cli.Call(context.Background(), &example_interfaces.AddTwoInts_Request{A: 40, B: 2})
	if err != nil || resp.Sum != 42 {
		t.Fatal(resp, err)
	}

	// Async call delivered on the client node's executor.
	done := make(chan struct{})
	cli.CallAsync(&example_interfaces.AddTwoInts_Request{A: 1, B: 1}, func(r *example_interfaces.AddTwoInts_Response, err error) {
		if err != nil || r.Sum != 2 {
			t.Error(r, err)
		}
		close(done)
	})
	if !SpinUntilFutureComplete(client, done, 2*time.Second) {
		t.Fatal("async timeout")
	}

	// Handler errors propagate.
	NewService(server, "fail", func(req *std_srvsTriggerReq, resp *std_srvsTriggerResp) error { return errTest })
	fc, _ := NewClient[std_srvsTriggerReq, std_srvsTriggerResp](client, "fail")
	fc.WaitForService(2 * time.Second)
	if _, err := fc.Call(context.Background(), &std_srvsTriggerReq{}); err == nil {
		t.Fatal("expected error")
	}

	// Unknown service.
	uc, _ := NewClient[std_srvsTriggerReq, std_srvsTriggerResp](client, "nope")
	if _, err := uc.Call(context.Background(), &std_srvsTriggerReq{}); err != ErrNoService {
		t.Fatal(err)
	}
	// Context cancellation while waiting.
	NewService(server, "slow", func(req *std_srvsTriggerReq, resp *std_srvsTriggerResp) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	sc, _ := NewClient[std_srvsTriggerReq, std_srvsTriggerResp](client, "slow")
	sc.WaitForService(2 * time.Second)
	cctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := sc.Call(cctx, &std_srvsTriggerReq{}); err != context.DeadlineExceeded {
		t.Fatal(err)
	}
}

type std_srvsTriggerReq = example_interfaces.Trigger_Request
type std_srvsTriggerResp = example_interfaces.Trigger_Response

type testErr struct{}

func (testErr) Error() string { return "boom" }

var errTest = testErr{}

func TestTimers(t *testing.T) {
	ctx, _ := Init(quiet()...)
	defer ctx.Shutdown()
	node, _ := ctx.NewNode("timers")
	var n atomic.Int32
	tm := node.NewTimer(10*time.Millisecond, func() { n.Add(1) })
	ex := NewSingleThreadedExecutor()
	ex.AddNode(node)
	go ex.Spin()
	defer ex.Cancel()
	waitFor(t, time.Second, func() bool { return n.Load() >= 3 })
	tm.Cancel()
	time.Sleep(30 * time.Millisecond)
	v := n.Load()
	time.Sleep(30 * time.Millisecond)
	if n.Load() != v {
		t.Fatal("timer fired after cancel")
	}
	var once atomic.Int32
	node.NewOneShotTimer(5*time.Millisecond, func() { once.Add(1) })
	time.Sleep(50 * time.Millisecond)
	if once.Load() != 1 {
		t.Fatal("one-shot", once.Load())
	}
}

func TestParameters(t *testing.T) {
	ctx, _ := Init(quiet(WithArgs([]string{"--ros-args", "-p", "rate:=20", "-p", "params:name:=\"bob\"", "-p", "other:x:=1", "-p", "gains:=[1.0, 2, 3]"}))...)
	defer ctx.Shutdown()
	node, _ := ctx.NewNode("params")
	v, err := node.DeclareParameter("rate", DoubleValue(5))
	if err != nil || v.Type != ParameterDouble || v.Double != 20 {
		t.Fatal(v, err)
	}
	v, _ = node.DeclareParameter("name", StringValue("alice"))
	if v.String != "bob" {
		t.Fatal(v)
	}
	if node.HasParameter("x") {
		t.Fatal("override for other node applied")
	}
	v, _ = node.DeclareParameter("gains", DoubleArrayValue(nil))
	if len(v.DoubleArray) != 3 || v.DoubleArray[1] != 2 {
		t.Fatal(v)
	}
	node.DeclareParameter("ro", IntegerValue(1), ParameterDescriptor{ReadOnly: true})
	if err := node.SetParameter(Parameter{"ro", IntegerValue(2)}); err == nil {
		t.Fatal("read-only")
	}
	if err := node.SetParameter(Parameter{"rate", StringValue("x")}); err == nil {
		t.Fatal("type mismatch")
	}
	node.AddOnSetParametersCallback(func(ps []Parameter) SetParametersResult {
		for _, p := range ps {
			if p.Name == "rate" && p.Value.Double > 100 {
				return SetParametersResult{false, "too fast"}
			}
		}
		return SetParametersResult{Successful: true}
	})
	if err := node.SetParameter(Parameter{"rate", DoubleValue(500)}); err == nil || err.Error() != "too fast" {
		t.Fatal(err)
	}
	if err := node.SetParameter(Parameter{"rate", IntegerValue(50)}); err != nil {
		t.Fatal(err)
	}
	if v, _ := node.GetParameter("rate"); v.Double != 50 {
		t.Fatal(v)
	}
	names := node.ListParameters(nil, 0)
	if len(names) != 4 {
		t.Fatal(names)
	}

	// Over the standard parameter services.
	go Spin(node)
	cn, _ := ctx.NewNode("param_client")
	gc, _ := NewClient[rcl_interfaces.GetParameters_Request, rcl_interfaces.GetParameters_Response](cn, "/params/get_parameters")
	resp, err := gc.Call(context.Background(), &rcl_interfaces.GetParameters_Request{Names: []string{"name", "rate"}})
	if err != nil || len(resp.Values) != 2 || resp.Values[0].StringValue != "bob" || resp.Values[1].DoubleValue != 50 {
		t.Fatal(resp, err)
	}
	sc, _ := NewClient[rcl_interfaces.SetParameters_Request, rcl_interfaces.SetParameters_Response](cn, "/params/set_parameters")
	sr, err := sc.Call(context.Background(), &rcl_interfaces.SetParameters_Request{Parameters: []rcl_interfaces.Parameter{{Name: "name", Value: StringValue("carol").ToMsg()}}})
	if err != nil || !sr.Results[0].Successful {
		t.Fatal(sr, err)
	}
	if v, _ := node.GetParameter("name"); v.String != "carol" {
		t.Fatal(v)
	}
	lc, _ := NewClient[rcl_interfaces.ListParameters_Request, rcl_interfaces.ListParameters_Response](cn, "/params/list_parameters")
	lr, err := lc.Call(context.Background(), &rcl_interfaces.ListParameters_Request{})
	if err != nil || len(lr.Result.Names) != 4 {
		t.Fatal(lr, err)
	}
}

func TestRemapping(t *testing.T) {
	ctx, _ := Init(quiet(WithArgs([]string{"--ros-args", "-r", "__ns:=/r1", "-r", "chatter:=talk", "-r", "foo:__node:=bar", "--", "x"}))...)
	defer ctx.Shutdown()
	if a := ctx.Args(); len(a) != 1 || a[0] != "x" {
		t.Fatal(a)
	}
	node, _ := ctx.NewNode("foo")
	if node.FullyQualifiedName() != "/r1/bar" {
		t.Fatal(node.FullyQualifiedName())
	}
	pub, _ := NewPublisher[*std_msgs.String](node, "chatter", QoSDefault)
	if pub.Topic() != "/r1/talk" {
		t.Fatal(pub.Topic())
	}
	p2, _ := NewPublisher[*std_msgs.String](node, "~/status", QoSDefault)
	if p2.Topic() != "/r1/bar/status" {
		t.Fatal(p2.Topic())
	}
	if _, err := ctx.NewNode("bad name"); err == nil {
		t.Fatal("invalid node name accepted")
	}
}

func TestRosoutAndLogging(t *testing.T) {
	ctx, _ := Init(quiet()...)
	defer ctx.Shutdown()
	node, _ := ctx.NewNode("logger")
	got := make(chan *rcl_interfaces.Log, 1)
	NewSubscription(node, "/rosout", QoSRosout, func(m *rcl_interfaces.Log) { got <- m })
	node.Logger().Warnf("value=%d", 7)
	node.Logger().Debug("hidden")
	SpinOnce(node, time.Second)
	select {
	case m := <-got:
		if m.Level != uint8(LogWarn) || m.Msg != "value=7" || m.Name != "logger" {
			t.Fatal(m)
		}
	default:
		t.Fatal("no rosout record")
	}
}

func TestUDPMulticast(t *testing.T) {
	ta, err := udp.New(udp.Options{Domain: 77})
	if err != nil {
		t.Skip("no multicast:", err)
	}
	tb, err := udp.New(udp.Options{Domain: 77})
	if err != nil {
		t.Skip("no multicast:", err)
	}
	a, _ := Init(quiet(WithTransport(ta), WithDomain(77))...)
	b, _ := Init(quiet(WithTransport(tb), WithDomain(77))...)
	defer a.Shutdown()
	defer b.Shutdown()
	na, _ := a.NewNode("udp_a")
	nb, _ := b.NewNode("udp_b")
	pub, _ := NewPublisher[*std_msgs.String](na, "udp", QoSDefault)
	got := make(chan string, 1)
	NewSubscription(nb, "udp", QoSDefault, func(m *std_msgs.String) { got <- m.Data })
	go Spin(nb)
	waitFor(t, 3*time.Second, func() bool { return pub.SubscriptionCount() == 1 })
	pub.Publish(&std_msgs.String{Data: "over udp"})
	select {
	case s := <-got:
		if s != "over udp" {
			t.Fatal(s)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestNodeDestroy(t *testing.T) {
	a, b := twoContexts(t)
	defer a.Shutdown()
	defer b.Shutdown()
	na, _ := a.NewNode("temp")
	NewPublisher[*std_msgs.String](na, "t", QoSDefault)
	nb, _ := b.NewNode("watcher")
	waitFor(t, 2*time.Second, func() bool { return len(nb.GetNodeNames()) == 2 })
	na.Destroy()
	waitFor(t, 2*time.Second, func() bool { return len(nb.GetNodeNames()) == 1 })
	if _, ok := nb.GetTopicNamesAndTypes()["/t"]; ok {
		t.Fatal("topic still visible")
	}
}

type pipeRW struct {
	*io.PipeReader
	*io.PipeWriter
}

func (p pipeRW) Close() error {
	p.PipeReader.Close()
	return p.PipeWriter.Close()
}

// A participant on a serial link (as a microcontroller would be) talks to a
// participant on the in-process bus through transport.Bridge.
func TestSerialBridge(t *testing.T) {
	devRead, hostWrite := io.Pipe()
	hostRead, devWrite := io.Pipe()
	devLink := serial.New(pipeRW{devRead, devWrite}, 0)
	hostLink := serial.New(pipeRW{hostRead, hostWrite}, 0)
	bus := inproc.NewBus(64)
	go transport.Bridge(hostLink, bus.Join())

	dev, _ := Init(quiet(WithTransport(devLink))...)
	host, _ := Init(quiet(WithTransport(bus.Join()))...)
	defer dev.Shutdown()
	defer host.Shutdown()

	mcu, _ := dev.NewNode("mcu", WithoutRosout(), WithoutParameterServices())
	pc, _ := host.NewNode("pc")
	imuPub, _ := NewPublisher[*sensor_msgs.Imu](mcu, "/imu", QoSSensorData)
	NewService(mcu, "/led", func(req *std_srvs.SetBool_Request, resp *std_srvs.SetBool_Response) error {
		resp.Success = req.Data
		return nil
	})
	go Spin(mcu)

	got := make(chan *sensor_msgs.Imu, 4)
	NewSubscription(pc, "/imu", QoSSensorData, func(m *sensor_msgs.Imu) { got <- m })
	go Spin(pc)
	waitFor(t, 3*time.Second, func() bool { return imuPub.SubscriptionCount() == 1 })
	imu := sensor_msgs.NewImu()
	imu.Header.FrameId = "imu_link"
	imu.LinearAcceleration.Z = 9.81
	imuPub.Publish(imu)
	select {
	case m := <-got:
		if m.Header.FrameId != "imu_link" || m.LinearAcceleration.Z != 9.81 || m.Orientation.W != 1 {
			t.Fatalf("%+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	cli, _ := NewClient[std_srvs.SetBool_Request, std_srvs.SetBool_Response](pc, "/led")
	if !cli.WaitForService(2 * time.Second) {
		t.Fatal("service")
	}
	resp, err := cli.Call(context.Background(), &std_srvs.SetBool_Request{Data: true})
	if err != nil || !resp.Success {
		t.Fatal(resp, err)
	}
	if n := pc.GetNodeNames(); len(n) != 2 {
		t.Fatal(n)
	}
}
