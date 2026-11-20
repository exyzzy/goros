package rtps_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/example_interfaces"
	"bitbucket.org/goros/msgs/std_msgs"
	"bitbucket.org/goros/transport/rtps"
)

const testDomain = 93

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for " + what)
}

func newCtx(t *testing.T) *goros.Context {
	t.Helper()
	tr, err := rtps.New(rtps.Options{Domain: testDomain, Debug: func(s string) { t.Log(s) }})
	if err != nil {
		t.Skip("rtps transport unavailable: " + err.Error())
	}
	ctx, err := goros.Init(goros.WithDomain(testDomain), goros.WithoutDefaultTransport(), goros.WithTransport(tr), goros.WithLogWriter(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ctx.Shutdown)
	return ctx
}

func spin(t *testing.T, nodes ...*goros.Node) {
	ex := goros.NewSingleThreadedExecutor()
	for _, n := range nodes {
		ex.AddNode(n)
	}
	go ex.Spin()
	t.Cleanup(ex.Cancel)
}

func TestPubSubOverRTPS(t *testing.T) {
	a, b := newCtx(t), newCtx(t)
	talker, _ := a.NewNode("talker")
	listener, _ := b.NewNode("listener")
	pub, err := goros.NewPublisher[*std_msgs.String](talker, "chatter", goros.QoSDefault)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var got []string
	_, err = goros.NewSubscription(listener, "chatter", goros.QoSDefault, func(m *std_msgs.String) {
		mu.Lock()
		got = append(got, m.Data)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	spin(t, talker, listener)

	// Node names travel over ros_discovery_info.
	waitFor(t, 10*time.Second, "listener to see talker node", func() bool {
		for _, n := range b.NodeNames() {
			if n == "/talker" {
				return true
			}
		}
		return false
	})
	waitFor(t, 10*time.Second, "subscription match", func() bool { return pub.SubscriptionCount() >= 1 })
	for i := 0; i < 5; i++ {
		pub.Publish(&std_msgs.String{Data: "hello " + string(rune('0'+i))})
		time.Sleep(20 * time.Millisecond)
	}
	waitFor(t, 5*time.Second, "5 samples", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= 5
	})
	mu.Lock()
	defer mu.Unlock()
	if got[0] != "hello 0" || got[4] != "hello 4" {
		t.Fatalf("unexpected samples %v", got)
	}
}

func TestTransientLocalOverRTPS(t *testing.T) {
	a, b := newCtx(t), newCtx(t)
	na, _ := a.NewNode("latched_pub")
	nb, _ := b.NewNode("late_sub")
	qos := goros.QoSProfile{History: goros.KeepLast, Depth: 10, Reliability: goros.Reliable, Durability: goros.TransientLocal}
	pub, _ := goros.NewPublisher[*std_msgs.Int32](na, "map", qos)
	spin(t, na, nb)
	for i := int32(1); i <= 3; i++ {
		pub.Publish(&std_msgs.Int32{Data: i})
	}
	// Let discovery settle before the late joiner arrives.
	waitFor(t, 10*time.Second, "participants to discover each other", func() bool {
		return len(b.TopicNamesAndTypes()["/map"]) > 0
	})
	var mu sync.Mutex
	var got []int32
	goros.NewSubscription(nb, "map", qos, func(m *std_msgs.Int32) {
		mu.Lock()
		got = append(got, m.Data)
		mu.Unlock()
	})
	waitFor(t, 10*time.Second, "latched samples", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= 3
	})
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("replay gave %v", got)
	}
}

func TestServiceOverRTPS(t *testing.T) {
	a, b := newCtx(t), newCtx(t)
	server, _ := a.NewNode("adder")
	client, _ := b.NewNode("caller")
	_, err := goros.NewService(server, "add_two_ints", func(req *example_interfaces.AddTwoInts_Request, resp *example_interfaces.AddTwoInts_Response) error {
		resp.Sum = req.A + req.B
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cli, err := goros.NewClient[example_interfaces.AddTwoInts_Request, example_interfaces.AddTwoInts_Response](client, "add_two_ints")
	if err != nil {
		t.Fatal(err)
	}
	spin(t, server, client)
	if !cli.WaitForService(10 * time.Second) {
		t.Fatal("service not discovered")
	}
	// Discovery of the service in the graph precedes endpoint matching on
	// the DDS side by a few round trips; retry briefly.
	deadline := time.Now().Add(10 * time.Second)
	for {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := cli.Call(cctx, &example_interfaces.AddTwoInts_Request{A: 40, B: 2})
		cancel()
		if err == nil {
			if resp.Sum != 42 {
				t.Fatalf("sum = %d", resp.Sum)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("call failed: " + err.Error())
		}
	}
	// A second call reuses the matched endpoints and must be quick.
	cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := cli.Call(cctx, &example_interfaces.AddTwoInts_Request{A: 1, B: 2})
	if err != nil || resp.Sum != 3 {
		t.Fatalf("second call: %v %v", resp, err)
	}
}
