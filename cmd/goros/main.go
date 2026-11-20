// Command goros is the command-line tool for a goros graph, modelled on
// the ros2 CLI:
//
//	goros node list
//	goros node info /talker
//	goros topic list [-t]
//	goros topic echo /chatter
//	goros topic pub [-r HZ] [-1] /chatter std_msgs/msg/String '{"data": "hi"}'
//	goros topic hz /chatter
//	goros topic info /chatter
//	goros service list [-t]
//	goros service call /add_two_ints example_interfaces/srv/AddTwoInts '{"a": 2, "b": 3}'
//	goros param list /node | get /node name | set /node name value
//	goros interface list | show std_msgs/msg/String
//	goros bridge --serial /dev/ttyACM0 [--udp-listen :7405] [--peer host:port]
//
// ROS_DOMAIN_ID and --ros-args are honoured like any other goros program.
package main

import (
	"fmt"
	"os"

	_ "bitbucket.org/goros/msgs/all"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage: goros <command> [args]

commands:
  node list | info <name>
  topic list [-t] | echo <topic> [type] | pub [-r hz] [-1] <topic> <type> <json> | hz <topic> | info <topic>
  service list [-t] | call <service> <type> <json>
  param list <node> | get <node> <name> | set <node> <name> <value> | describe <node> <name>
  interface list | show <type>
  bridge --serial <dev> [--udp-listen addr] [--peer host:port] [--no-multicast]
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	args := os.Args[2:]
	var err error
	switch os.Args[1] {
	case "node":
		err = cmdNode(args)
	case "topic":
		err = cmdTopic(args)
	case "service":
		err = cmdService(args)
	case "param":
		err = cmdParam(args)
	case "interface":
		err = cmdInterface(args)
	case "bridge":
		err = cmdBridge(args)
	case "help", "-h", "--help":
		usage()
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "goros:", err)
		os.Exit(1)
	}
}
