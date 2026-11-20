// add_two_ints_client calls the add_two_ints service once and exits.
//
//	add_two_ints_client 2 3
package main

import (
	"context"
	"os"
	"strconv"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/example_interfaces"
)

func main() {
	ctx, err := goros.Init(goros.WithArgs(os.Args[1:]))
	if err != nil {
		panic(err)
	}
	defer ctx.Shutdown()

	a, b := int64(2), int64(3)
	if args := ctx.Args(); len(args) == 2 {
		a, _ = strconv.ParseInt(args[0], 10, 64)
		b, _ = strconv.ParseInt(args[1], 10, 64)
	}
	node, _ := ctx.NewNode("add_two_ints_client")
	cli, _ := goros.NewClient[example_interfaces.AddTwoInts_Request, example_interfaces.AddTwoInts_Response](node, "add_two_ints")
	for !cli.WaitForService(time.Second) {
		node.Logger().Info("service not available, waiting again...")
	}
	resp, err := cli.Call(context.Background(), &example_interfaces.AddTwoInts_Request{A: a, B: b})
	if err != nil {
		node.Logger().Error("call failed: " + err.Error())
		os.Exit(1)
	}
	node.Logger().Info("Result of add_two_ints: " + strconv.FormatInt(resp.Sum, 10))
}
