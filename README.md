# goros

A ROS 2 client library in Go that builds with both the standard Go toolchain
and [TinyGo](https://tinygo.org), so the same node code runs on a laptop,
a Raspberry Pi, a WebAssembly host or a microcontroller with 8 KB of RAM.

goros ports the ROS 2 *programming model* described at
[docs.ros.org](https://docs.ros.org/): contexts, nodes, topics, typed
publishers and subscriptions, services and clients, timers, executors,
parameters (with the standard parameter services), QoS profiles, loggers
with `/rosout`, graph introspection, name remapping and `--ros-args`. Messages
are generated from ordinary `.msg`/`.srv` files and serialized with CDR,
byte-for-byte what `rclcpp` produces.

On hosts goros speaks DDS-RTPS itself (`transport/rtps`): a goros node is
a participant in the same graph as `rclcpp` and `rclpy` nodes, so `ros2
topic echo`, `ros2 node list` and `ros2 service call` work against it and
`goros topic echo` works against them. No DDS library is linked; the
transport is a few thousand lines of Go. Microcontrollers reach the graph
over a framed serial link through `goros bridge`, or directly over unicast
RTPS when they have Wi-Fi. See *Status* for what has been verified.

```
┌──────────────┐    DDS-RTPS      ┌──────────────┐   serial/COBS   ┌──────────────┐
│ rclcpp nodes │◄────────────────►│ goros bridge │◄───────────────►│ pico (TinyGo)│
│ ros2 CLI     │  SPDP/SEDP/DATA  │  (host)      │  /dev/ttyACM0   │ sensor node  │
│ goros nodes  │                  └──────────────┘                 └──────────────┘
│ goros CLI    │
└──────────────┘
```

## Quick start

```sh
go build ./cmd/goros ./examples/...

./talker &                          # publishes std_msgs/String on /chatter
./listener --ros-args -r __ns:=/robot1
./goros node list
./goros topic list -t
./goros topic echo /chatter
./goros service call /add_two_ints example_interfaces/srv/AddTwoInts '{"a": 2, "b": 3}'
./goros param set /param_demo rate 5
```

A talker in full:

```go
package main

import (
	"os"
	"strconv"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/std_msgs"
)

func main() {
	ctx, _ := goros.Init(goros.WithArgs(os.Args[1:]))
	defer ctx.Shutdown()

	node, _ := ctx.NewNode("talker")
	pub, _ := goros.NewPublisher[*std_msgs.String](node, "chatter", goros.QoSDefault)
	count := 0
	node.NewTimer(500*time.Millisecond, func() {
		count++
		pub.Publish(&std_msgs.String{Data: "Hello World: " + strconv.Itoa(count)})
	})
	goros.Spin(node)
}
```

The listener side infers the message type from the callback:

```go
goros.NewSubscription(node, "chatter", goros.QoSDefault, func(m *std_msgs.String) {
	node.Logger().Info("I heard: '" + m.Data + "'")
})
```

Services and clients:

```go
goros.NewService(node, "add_two_ints",
	func(req *example_interfaces.AddTwoInts_Request, resp *example_interfaces.AddTwoInts_Response) error {
		resp.Sum = req.A + req.B
		return nil
	})

cli, _ := goros.NewClient[example_interfaces.AddTwoInts_Request, example_interfaces.AddTwoInts_Response](node, "add_two_ints")
cli.WaitForService(time.Second)
resp, err := cli.Call(ctx, &example_interfaces.AddTwoInts_Request{A: 2, B: 3})
```

## TinyGo

The core library, the message packages and the serial/UDP transports compile
under TinyGo 0.44 for host, WebAssembly and bare-metal targets. The example
firmware in `examples/tinygo/pico_sensor` publishes `sensor_msgs/Imu` and
`sensor_msgs/Temperature` over USB serial and serves a `std_srvs/SetBool`
service that toggles the LED. Measured with `tinygo build -size short`:

| Target            | Flash   | Static RAM |
|-------------------|---------|------------|
| pico / pico-w     | 77 KB   | 7.4 KB     |
| esp32             | 84 KB   | 6.3 KB     |
| feather-nrf52840  | 123 KB  | 6.8 KB     |

```sh
tinygo flash -target pico ./examples/tinygo/pico_sensor

# on the host
stty -f /dev/tty.usbmodem1101 115200 raw      # macOS; use stty -F on Linux
goros bridge --serial /dev/tty.usbmodem1101
goros topic echo /imu/data_raw
goros service call /led std_srvs/srv/SetBool '{"data": true}'
```

`goros bridge` is the goros analogue of the micro-ROS agent: it forwards
frames between any set of transports, so a board on a serial link becomes a
regular participant in the ROS 2 graph, with its node, topics and services
visible to `ros2` and to every goros process. Boards with Wi-Fi (Pico W,
ESP32 via `tinygo.org/x/drivers/netlink`) can instead speak RTPS directly
with `rtps.New(rtps.Options{Peers: []string{"<host>"}, Advertise: ...})`
(no multicast on TinyGo, so the host is named explicitly, like
`ROS_STATIC_PEERS`), or use the lighter goros-native `transport/udp` in
unicast mode against `goros bridge --udp-listen :7405`.

Things to know when targeting microcontrollers:

- TinyGo has no multicast and its `net` package needs a network driver, so
  `goros.Init` attaches no default transport there; pass one with
  `goros.WithTransport`.
- `serial.Adapt(machine.Serial)` turns TinyGo's `machine.Serialer` into the
  `io.ReadWriter` the serial transport wants.
- Pass `goros.WithLogWriter(nil)` when the USB port carries goros frames,
  and `goros.WithoutRosout()`/`WithoutParameterServices()` to the node to
  keep the firmware small.
- Prefer `Logger.Info` and `strconv` over the `*f` logging variants to keep
  `fmt` out of the binary.
- `crypto/rand` is used for participant GUIDs where the target provides it;
  otherwise a clock-seeded fallback is used.

Installing TinyGo on macOS without Homebrew: download
`tinygo<version>.darwin-arm64.tar.gz` from the
[releases page](https://github.com/tinygo-org/tinygo/releases), extract it
anywhere and put `tinygo/bin` on `PATH`. The Makefile accepts `TINYGO=path`.

## Documentation

- [ROS 2 to goros mapping](docs/ros2-to-goros.md): what each ROS 2 concept and `ros2` command is called here, and where behaviour differs. Start here if you know ROS 2.
- [API guide](docs/api-guide.md): rclpy snippets next to their goros equivalents.
- [Transports and TinyGo](docs/transports-and-tinygo.md): wire frames, the built-in transports, the bridge, and firmware guidance.
- [Status and roadmap](docs/status.md): feature matrix, test coverage, limitations.

Concepts are documented at [docs.ros.org](https://docs.ros.org/en/jazzy/); goros follows them and the mapping page links to the relevant sections.

## Layout

| Path                   | Contents                                                    |
|------------------------|-------------------------------------------------------------|
| `goros.go`, `node.go`… | Context, Node, Publisher, Subscription, Service, Client, Timer, Executor, parameters, logging, graph |
| `cdr/`                 | CDR (XCDR1) encoder/decoder, little- and big-endian          |
| `msg/`                 | `Message` interface, type registry                           |
| `names/`               | Topic/node/namespace validation and expansion                |
| `transport/`           | Wire frames, `Bridge`; `rtps` (DDS), `udp`, `serial`, `inproc` transports |
| `interfaces/`          | `.msg`/`.srv` sources (std_msgs, geometry_msgs, sensor_msgs, nav_msgs, rcl_interfaces, example_interfaces, std_srvs, builtin_interfaces) |
| `msgs/`                | Generated Go packages (`go generate ./msgs`)                 |
| `cmd/msggen`           | The generator                                                |
| `cmd/goros`            | CLI: `node`, `topic`, `service`, `param`, `interface`, `bridge` |
| `examples/`            | talker, listener, add_two_ints server/client, parameters, serial host, Pico firmware, `rtps_interop` probe |
| `tools/cyclone_interop` | Python script that checks interop against Cyclone DDS without a ROS install |

### Adding message types

Drop `.msg`/`.srv` files under `interfaces/<pkg>/msg|srv/` and run
`go generate ./msgs`. The generator handles primitives, bounded/unbounded
strings and sequences, fixed arrays, nested and cross-package types, constants
and default values, and emits `TypeName`, `MarshalCDR`, `UnmarshalCDR`,
`New<Type>()`, `SetDefaults()` plus registry registration for every type.

## Status

Implemented, with tests:

- Nodes, namespaces, `~/private` names, `{node}`/`{ns}` substitutions,
  `--ros-args -r a:=b -r __node:=x -r __ns:=/y -p name:=value --log-level`.
- Publishers/subscriptions with KeepLast depth, Reliable/BestEffort and
  Volatile/TransientLocal matching rules, transient-local replay to late
  joiners (local and remote).
- Services/clients: synchronous `Call` with `context.Context`, `CallAsync`,
  `WaitForService`, handler errors reported to the caller, raw (untyped)
  variants for tooling.
- Wall timers, one-shot timers, `Rate`.
- Single- and multi-threaded executors, `Spin`, `SpinOnce`, `SpinSome`,
  `SpinUntilFutureComplete`.
- Parameters: declare/get/set/undeclare, descriptors, read-only, type
  checking with integer→double coercion, validation callbacks, overrides
  from the command line, `/parameter_events`, and the six standard
  `~/*_parameters` services so `goros param` works against any node.
- Logging in the rcutils console format, forwarded to `/rosout`.
- Discovery: nodes, topics, services, per-endpoint QoS, publisher and
  subscriber counts, participant departure and lease expiry.
- CDR wire compatibility with rclcpp for the generated message types.
- **DDS-RTPS transport** (default on hosts): SPDP/SEDP discovery, reliable
  and best-effort writers and readers with heartbeat/acknack
  retransmission, transient-local history, DATA_FRAG for large samples,
  ROS 2 topic/type name mangling, `ros_discovery_info` node announcement,
  services with Fast DDS/Connext request correlation. Verified in both
  directions against Cyclone DDS 0.10 (topics, latched topics, services,
  node and endpoint discovery); not yet exercised against a Fast DDS
  install, which is the Jazzy default. `tools/cyclone_interop` reproduces
  the check with nothing but `pip install cyclonedds`.

Not implemented (yet):

- Actions, lifecycle nodes, ROS time / `use_sim_time`, callback groups,
  `rosbag`, `tf2`, launch files, SROS 2 security, DDS shared memory.
- Services against `rmw_cyclonedds` nodes: Cyclone's rmw puts the request
  header inside the payload, a convention that is also incompatible with
  Fast DDS; topics work with every vendor.
- Reliable delivery on the goros-native UDP transport. Reliability is
  honoured for matching only there; RTPS, serial, in-process and
  intra-context delivery are reliable.

## Development

```sh
make test          # go test -race ./...
make generate      # regenerate msgs/ from interfaces/
make build         # CLI and examples into bin/
make tinygo        # host build of the examples with TinyGo
make pico          # firmware for the Pico (TINYGO_TARGET=pico-w etc.)
```
