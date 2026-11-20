// Listener prints every std_msgs/String received on /chatter.
package main

import (
	"os"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/std_msgs"
)

func main() {
	ctx, err := goros.Init(goros.WithArgs(os.Args[1:]))
	if err != nil {
		panic(err)
	}
	defer ctx.Shutdown()

	node, err := ctx.NewNode("listener")
	if err != nil {
		panic(err)
	}
	_, err = goros.NewSubscription(node, "chatter", goros.QoSDefault, func(m *std_msgs.String) {
		node.Logger().Info("I heard: '" + m.Data + "'")
	})
	if err != nil {
		panic(err)
	}
	goros.Spin(node)
}
