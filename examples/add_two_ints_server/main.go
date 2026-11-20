// add_two_ints_server answers example_interfaces/srv/AddTwoInts requests.
package main

import (
	"os"
	"strconv"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/example_interfaces"
)

func main() {
	ctx, err := goros.Init(goros.WithArgs(os.Args[1:]))
	if err != nil {
		panic(err)
	}
	defer ctx.Shutdown()

	node, _ := ctx.NewNode("add_two_ints_server")
	_, err = goros.NewService(node, "add_two_ints", func(req *example_interfaces.AddTwoInts_Request, resp *example_interfaces.AddTwoInts_Response) error {
		node.Logger().Info("Incoming request: a=" + strconv.FormatInt(req.A, 10) + " b=" + strconv.FormatInt(req.B, 10))
		resp.Sum = req.A + req.B
		return nil
	})
	if err != nil {
		panic(err)
	}
	goros.Spin(node)
}
