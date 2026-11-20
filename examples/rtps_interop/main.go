// rtps_interop exercises goros against a real DDS participant: it talks on
// /chatter, latches three samples on /latched, listens on /to_goros and
// serves /add_two_ints, printing what it sees in the graph. Run it next to
// `ros2 run demo_nodes_cpp listener` or the Cyclone DDS script in the
// repository's test notes.
package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/example_interfaces"
	"bitbucket.org/goros/msgs/std_msgs"
	"bitbucket.org/goros/transport/rtps"
)

func main() {
	domain := 0
	if v := os.Getenv("ROS_DOMAIN_ID"); v != "" {
		domain, _ = strconv.Atoi(v)
	}
	runFor := 20 * time.Second
	if len(os.Args) > 1 {
		if d, err := time.ParseDuration(os.Args[1]); err == nil {
			runFor = d
		}
	}
	var debug func(string)
	if os.Getenv("GOROS_RTPS_DEBUG") != "" {
		debug = func(s string) { fmt.Fprintln(os.Stderr, "rtps:", s) }
	}
	tr, err := rtps.New(rtps.Options{Domain: domain, Debug: debug})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, err := goros.Init(goros.WithDomain(domain), goros.WithoutDefaultTransport(), goros.WithTransport(tr), goros.WithArgs(os.Args[1:]))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer ctx.Shutdown()
	node, _ := ctx.NewNode("goros_interop")
	pub, _ := goros.NewPublisher[*std_msgs.String](node, "chatter", goros.QoSDefault)
	latched, _ := goros.NewPublisher[*std_msgs.Int32](node, "latched", goros.QoSProfile{History: goros.KeepLast, Depth: 10, Reliability: goros.Reliable, Durability: goros.TransientLocal})
	for i := int32(1); i <= 3; i++ {
		latched.Publish(&std_msgs.Int32{Data: i})
	}
	goros.NewSubscription(node, "to_goros", goros.QoSDefault, func(m *std_msgs.String) {
		fmt.Println("goros got:", m.Data)
	})
	goros.NewSubscription(node, "latched_from_dds", goros.QoSProfile{History: goros.KeepLast, Depth: 10, Reliability: goros.Reliable, Durability: goros.TransientLocal}, func(m *std_msgs.Int32) {
		fmt.Println("goros got latched:", m.Data)
	})
	goros.NewService(node, "add_two_ints", func(req *example_interfaces.AddTwoInts_Request, resp *example_interfaces.AddTwoInts_Response) error {
		resp.Sum = req.A + req.B
		fmt.Println("goros served add_two_ints:", req.A, "+", req.B)
		return nil
	})
	n := 0
	node.NewTimer(500*time.Millisecond, func() {
		n++
		pub.Publish(&std_msgs.String{Data: "hello from goros " + strconv.Itoa(n)})
	})
	node.NewTimer(5*time.Second, func() {
		names := ctx.NodeNames()
		sort.Strings(names)
		fmt.Println("graph nodes:", names)
		topics := ctx.TopicNamesAndTypes()
		keys := make([]string, 0, len(topics))
		for k := range topics {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Println("  topic", k, topics[k])
		}
		fmt.Println("  /chatter subscribers:", pub.SubscriptionCount())
	})
	go func() {
		time.Sleep(runFor)
		ctx.Shutdown()
	}()
	goros.Spin(node)
}
