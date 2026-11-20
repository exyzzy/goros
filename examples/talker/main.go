// Talker publishes "Hello World: N" on /chatter twice a second, like the
// demo_nodes_cpp talker.
package main

import (
	"os"
	"strconv"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/std_msgs"
)

func main() {
	ctx, err := goros.Init(goros.WithArgs(os.Args[1:]))
	if err != nil {
		panic(err)
	}
	defer ctx.Shutdown()

	node, err := ctx.NewNode("talker")
	if err != nil {
		panic(err)
	}
	pub, err := goros.NewPublisher[*std_msgs.String](node, "chatter", goros.QoSDefault)
	if err != nil {
		panic(err)
	}
	count := 0
	node.NewTimer(500*time.Millisecond, func() {
		count++
		m := &std_msgs.String{Data: "Hello World: " + strconv.Itoa(count)}
		node.Logger().Info("Publishing: '" + m.Data + "'")
		pub.Publish(m)
	})
	goros.Spin(node)
}
