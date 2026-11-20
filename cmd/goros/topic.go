package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msg"
)

func cmdTopic(args []string) error {
	if len(args) == 0 {
		return errors.New("topic: expected list, echo, pub, hz or info")
	}
	sub := args[0]
	args = args[1:]
	switch sub {
	case "list":
		rest, set, _, err := parseFlags(args, []string{"-t", "--show-types", "-a", "--all"}, nil)
		if err != nil {
			return err
		}
		_ = rest
		ctx, node, err := newCLIContext("topic")
		if err != nil {
			return err
		}
		defer ctx.Shutdown()
		waitDiscovery(ctx)
		topics := node.GetTopicNamesAndTypes()
		for _, t := range sortedKeys(topics) {
			if hidden(t) && !(set["-a"] || set["--all"]) {
				continue
			}
			if set["-t"] || set["--show-types"] {
				fmt.Printf("%s [%s]\n", t, strings.Join(topics[t], ", "))
			} else {
				fmt.Println(t)
			}
		}
		return nil
	case "echo":
		return topicEcho(args)
	case "pub":
		return topicPub(args)
	case "hz":
		return topicHz(args)
	case "info":
		return topicInfo(args)
	}
	return errors.New("topic: unknown subcommand " + sub)
}

// resolveTopicType waits for discovery and returns the type published on
// topic, unless one was given explicitly.
func resolveTopicType(ctx *goros.Context, node *goros.Node, topic, explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if types := node.GetTopicNamesAndTypes()[topic]; len(types) > 0 {
			return types[0], nil
		}
		if time.Now().After(deadline) || !ctx.OK() {
			return "", errors.New("could not determine the type for topic " + topic + " (is anything publishing it? pass the type explicitly)")
		}
		select {
		case <-ctx.GraphChanged():
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func topicEcho(args []string) error {
	rest, set, vals, err := parseFlags(args, []string{"--raw"}, []string{"--qos-reliability", "--qos-durability"})
	if err != nil {
		return err
	}
	if len(rest) < 1 {
		return errors.New("topic echo: expected <topic> [type]")
	}
	ctx, node, err := newCLIContext("echo")
	if err != nil {
		return err
	}
	defer ctx.Shutdown()
	onInterrupt(ctx)
	topic := rest[0]
	explicit := ""
	if len(rest) > 1 {
		explicit = rest[1]
	}
	typ, err := resolveTopicType(ctx, node, topic, explicit)
	if err != nil {
		return err
	}
	qos := goros.QoSSensorData // best-effort + volatile matches every publisher
	qos.Depth = 50
	if vals["--qos-durability"] == "transient_local" {
		qos.Durability = goros.TransientLocal
	}
	if vals["--qos-reliability"] == "reliable" {
		qos.Reliability = goros.Reliable
	}
	raw := set["--raw"]
	_, err = goros.NewRawSubscription(node, topic, typ, qos, func(payload []byte) {
		if raw {
			fmt.Printf("% x\n---\n", payload)
			return
		}
		m := msg.New(typ)
		if m == nil {
			fmt.Printf("<%d bytes of %s (type not registered in this build)>\n---\n", len(payload), typ)
			return
		}
		if err := msg.Decode(payload, m); err != nil {
			fmt.Println("decode error:", err)
			return
		}
		fmt.Println(toJSON(m))
		fmt.Println("---")
	})
	if err != nil {
		return err
	}
	return goros.Spin(node)
}

func topicPub(args []string) error {
	rest, set, vals, err := parseFlags(args, []string{"-1", "--once", "--transient-local"}, []string{"-r", "--rate"})
	if err != nil {
		return err
	}
	if len(rest) < 2 {
		return errors.New("topic pub: expected <topic> <type> [json]")
	}
	topic, typ := rest[0], rest[1]
	text := ""
	if len(rest) > 2 {
		text = strings.Join(rest[2:], " ")
	}
	m := msg.New(typ)
	if m == nil {
		return errors.New("unknown message type " + typ)
	}
	if err := fromJSON(text, m); err != nil {
		return err
	}
	rate := 1.0
	if r := vals["-r"] + vals["--rate"]; r != "" {
		if rate, err = strconv.ParseFloat(r, 64); err != nil {
			return err
		}
	}
	ctx, node, err := newCLIContext("pub")
	if err != nil {
		return err
	}
	defer ctx.Shutdown()
	onInterrupt(ctx)
	qos := goros.QoSDefault
	if set["--transient-local"] {
		qos.Durability = goros.TransientLocal
	}
	pub, err := goros.NewRawPublisher(node, topic, typ, qos)
	if err != nil {
		return err
	}
	payload := msg.Encode(m)
	once := set["-1"] || set["--once"]
	// Give subscribers a moment to discover us, as `ros2 topic pub` does.
	fmt.Println("publisher: beginning loop")
	waitDiscovery(ctx)
	r := goros.NewRate(rate)
	for ctx.OK() {
		pub.PublishRaw(payload)
		fmt.Println("publishing", typ)
		fmt.Println(toJSON(m))
		fmt.Println()
		if once {
			time.Sleep(200 * time.Millisecond) // let the frame leave
			return nil
		}
		r.Sleep()
	}
	return nil
}

func topicHz(args []string) error {
	if len(args) < 1 {
		return errors.New("topic hz: expected <topic>")
	}
	ctx, node, err := newCLIContext("hz")
	if err != nil {
		return err
	}
	defer ctx.Shutdown()
	onInterrupt(ctx)
	typ, err := resolveTopicType(ctx, node, args[0], "")
	if err != nil {
		return err
	}
	var mu sync.Mutex
	var stamps []time.Time
	_, err = goros.NewRawSubscription(node, args[0], typ, goros.QoSSensorData, func([]byte) {
		mu.Lock()
		stamps = append(stamps, time.Now())
		mu.Unlock()
	})
	if err != nil {
		return err
	}
	go goros.Spin(node)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.OK() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
		mu.Lock()
		window := append([]time.Time(nil), stamps...)
		if len(stamps) > 0 {
			stamps = stamps[len(stamps)-1:]
		}
		mu.Unlock()
		if len(window) < 2 {
			fmt.Println("no new messages")
			continue
		}
		var sum, min, max time.Duration
		min = time.Hour
		for i := 1; i < len(window); i++ {
			d := window[i].Sub(window[i-1])
			sum += d
			if d < min {
				min = d
			}
			if d > max {
				max = d
			}
		}
		avg := sum / time.Duration(len(window)-1)
		fmt.Printf("average rate: %.3f\n\tmin: %.3fs max: %.3fs window: %d\n", 1/avg.Seconds(), min.Seconds(), max.Seconds(), len(window))
	}
	return nil
}

func topicInfo(args []string) error {
	rest, set, _, err := parseFlags(args, []string{"-v", "--verbose"}, nil)
	if err != nil || len(rest) < 1 {
		return errors.New("topic info: expected <topic>")
	}
	ctx, node, err := newCLIContext("info")
	if err != nil {
		return err
	}
	defer ctx.Shutdown()
	waitDiscovery(ctx)
	topic := rest[0]
	types := node.GetTopicNamesAndTypes()[topic]
	if len(types) == 0 {
		return errors.New("unknown topic " + topic)
	}
	pubs, subs := ctx.PublishersInfo(topic), ctx.SubscriptionsInfo(topic)
	fmt.Printf("Type: %s\n\n", strings.Join(types, ", "))
	fmt.Printf("Publisher count: %d\n", len(pubs))
	if set["-v"] || set["--verbose"] {
		for _, e := range pubs {
			printEndpoint(e)
		}
	}
	fmt.Printf("\nSubscription count: %d\n", len(subs))
	if set["-v"] || set["--verbose"] {
		for _, e := range subs {
			printEndpoint(e)
		}
	}
	return nil
}

func printEndpoint(e goros.TopicEndpointInfo) {
	fmt.Printf("\nNode name: %s\nNode namespace: %s\nTopic type: %s\nParticipant: %s\nQoS profile:\n  Reliability: %s\n  Durability: %s\n  History: %s (depth %d)\n",
		e.NodeName, e.NodeNamespace, e.TopicType, e.Participant.String(), relName(e.QoS.Reliability), durName(e.QoS.Durability), histName(e.QoS.History), e.QoS.Depth)
}

func relName(r goros.Reliability) string {
	if r == goros.BestEffort {
		return "BEST_EFFORT"
	}
	return "RELIABLE"
}
func durName(d goros.Durability) string {
	if d == goros.TransientLocal {
		return "TRANSIENT_LOCAL"
	}
	return "VOLATILE"
}
func histName(h goros.History) string {
	if h == goros.KeepAll {
		return "KEEP_ALL"
	}
	return "KEEP_LAST"
}

var _ = os.Stdout
