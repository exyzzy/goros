# goros API guide for rclpy and rclcpp users

The ROS 2 client library tutorials
([Python](https://docs.ros.org/en/jazzy/Tutorials/Beginner-Client-Libraries/Writing-A-Simple-Py-Publisher-And-Subscriber/Writing-A-Simple-Py-Publisher-And-Subscriber.html),
[C++](https://docs.ros.org/en/jazzy/Tutorials/Beginner-Client-Libraries/Writing-A-Simple-Cpp-Publisher-And-Subscriber/Writing-A-Simple-Cpp-Publisher-And-Subscriber.html))
explain what each piece is for. This guide shows the Go spelling of each
piece next to the rclpy version. Reference detail is in godoc:
`go doc bitbucket.org/goros`.

Conventions that apply throughout:

- Constructors return `(value, error)`. The only panics are programming
  errors such as a nil node.
- Message types are pointers: `*std_msgs.String`. Generic APIs that need to
  *send* take the pointer type (`NewPublisher[*std_msgs.String]`); APIs that
  need to *allocate* take the value type or infer it from a callback.
- Field names are the ROS names in CamelCase: `frame_id` becomes `FrameId`,
  `linear_acceleration` becomes `LinearAcceleration`. Constants are
  `Type_NAME`, for example `sensor_msgs.NavSatStatus_STATUS_FIX`.
- Every generated type has `New<Type>()` which applies `.msg` defaults, so
  `geometry_msgs.NewQuaternion().W == 1`. A zero-value struct literal does
  not apply defaults.

## Initialisation and shutdown

rclpy:

```python
rclpy.init(args=sys.argv)
node = rclpy.create_node('talker', namespace='/robot1')
...
node.destroy_node()
rclpy.shutdown()
```

goros:

```go
ctx, err := goros.Init(goros.WithArgs(os.Args[1:]))
if err != nil {
	log.Fatal(err)
}
defer ctx.Shutdown()

node, err := ctx.NewNode("talker", goros.WithNamespace("/robot1"))
```

`Init` options you will use: `WithArgs` (enables `--ros-args`), `WithDomain`,
`WithTransport`, `WithoutDefaultTransport`, `WithLogWriter`, `WithLogLevel`.
Node options: `WithNamespace`, `WithParameterOverrides`, `WithoutRosout`,
`WithoutParameterServices`, `WithoutParameterEvents`, `WithClock`,
`WithNodeQueueSize`.

`ctx.OK()` is `rclpy.ok()`. `ctx.Done()` is a channel closed on shutdown,
convenient in `select`. `ctx.Args()` returns the non-ROS arguments left after
`--ros-args` processing, the equivalent of `rclpy.utilities.remove_ros_args`.

## Publishing

rclpy:

```python
self.pub = self.create_publisher(String, 'chatter', 10)
msg = String()
msg.data = 'Hello'
self.pub.publish(msg)
```

goros:

```go
pub, err := goros.NewPublisher[*std_msgs.String](node, "chatter", goros.QoSDefault)
pub.Publish(&std_msgs.String{Data: "Hello"})
```

The integer `10` in rclpy is a KeepLast depth; the goros equivalent is
`goros.KeepLastQoS(10)`, which is what `QoSDefault` already is. Reusing one
message struct across publishes is fine: `Publish` serializes synchronously
and does not retain the pointer.

For messages with a header:

```go
imu := sensor_msgs.NewImu()
imu.Header.FrameId = "imu_link"
imu.Header.Stamp = node.Now()
imu.LinearAcceleration.Z = 9.81
imuPub.Publish(imu)
```

## Subscribing

rclpy:

```python
self.sub = self.create_subscription(String, 'chatter', self.on_msg, 10)

def on_msg(self, msg):
    self.get_logger().info('I heard: "%s"' % msg.data)
```

goros:

```go
sub, err := goros.NewSubscription(node, "chatter", goros.QoSDefault, func(m *std_msgs.String) {
	node.Logger().Info("I heard: '" + m.Data + "'")
})
```

The message type is inferred from the callback parameter. The callback runs
on the executor that spins `node`, never concurrently with itself on a
single-threaded executor. `sub.Dropped()` counts samples discarded because
the KeepLast queue overflowed before the executor got to them.

Use `goros.NewRawSubscription(node, topic, typeName, qos, func(payload []byte))`
when you want the CDR bytes without decoding, for bridges and recorders.

## Spinning

rclpy:

```python
rclpy.spin(node)
# or
executor = MultiThreadedExecutor()
executor.add_node(node)
executor.spin()
```

goros:

```go
goros.Spin(node)            // blocks until ctx.Shutdown()

ex := goros.NewMultiThreadedExecutor(4)
ex.AddNode(node)
ex.Spin()
```

`goros.SpinOnce(node, timeout)` runs at most one callback.
`Executor.SpinSome(d)` drains what is ready. `Executor.Cancel()` makes
`Spin` return. `goros.SpinUntilFutureComplete(node, done, timeout)` spins
until a channel is closed, the goros replacement for
`rclpy.spin_until_future_complete`.

Shutting down from a signal handler:

```go
sig := make(chan os.Signal, 1)
signal.Notify(sig, os.Interrupt)
go func() { <-sig; ctx.Shutdown() }()
goros.Spin(node)
```

## Timers

rclpy:

```python
self.timer = self.create_timer(0.5, self.on_timer)
```

goros:

```go
timer := node.NewTimer(500*time.Millisecond, func() { ... })
timer.Cancel()
timer.Reset()
node.NewOneShotTimer(2*time.Second, func() { ... })
```

Timer callbacks run on the node's executor like any other callback.

## Services

rclpy server:

```python
self.srv = self.create_service(AddTwoInts, 'add_two_ints', self.handle)

def handle(self, request, response):
    response.sum = request.a + request.b
    return response
```

goros server:

```go
srv, err := goros.NewService(node, "add_two_ints",
	func(req *example_interfaces.AddTwoInts_Request, resp *example_interfaces.AddTwoInts_Response) error {
		resp.Sum = req.A + req.B
		return nil
	})
```

Request and response types are inferred from the handler. Returning a
non-nil error fails the call on the client side with that message.

rclpy client:

```python
self.cli = self.create_client(AddTwoInts, 'add_two_ints')
while not self.cli.wait_for_service(timeout_sec=1.0):
    self.get_logger().info('service not available, waiting again...')
future = self.cli.call_async(req)
rclpy.spin_until_future_complete(self, future)
result = future.result()
```

goros client:

```go
cli, err := goros.NewClient[example_interfaces.AddTwoInts_Request, example_interfaces.AddTwoInts_Response](node, "add_two_ints")
for !cli.WaitForService(time.Second) {
	node.Logger().Info("service not available, waiting again...")
}
resp, err := cli.Call(context.Background(), &example_interfaces.AddTwoInts_Request{A: 2, B: 3})
```

`Call` blocks and honours the `context.Context` deadline or cancellation.
Do not call it from inside a callback on the same single-threaded executor
that runs the service; that deadlocks, as in rclcpp. From a callback use:

```go
cli.CallAsync(req, func(resp *example_interfaces.AddTwoInts_Response, err error) {
	// runs on node's executor
})
```

`Call` returns `goros.ErrNoService` immediately if no matching service is
known, rather than waiting, so call `WaitForService` first when the server
may start later.

## Parameters

rclpy:

```python
self.declare_parameter('rate', 10.0)
rate = self.get_parameter('rate').value
self.add_on_set_parameters_callback(self.validate)
```

goros:

```go
rate, err := node.DeclareParameter("rate", goros.DoubleValue(10),
	goros.ParameterDescriptor{Description: "publish rate in Hz"})
v, err := node.GetParameter("rate")   // v.Type == goros.ParameterDouble, v.Double
node.AddOnSetParametersCallback(func(ps []goros.Parameter) goros.SetParametersResult {
	for _, p := range ps {
		if p.Name == "rate" && p.Value.Double <= 0 {
			return goros.SetParametersResult{Successful: false, Reason: "rate must be positive"}
		}
	}
	return goros.SetParametersResult{Successful: true}
})
```

`ParameterValue` is a tagged union with constructors `BoolValue`,
`IntegerValue`, `DoubleValue`, `StringValue`, `ByteArrayValue`,
`BoolArrayValue`, `IntegerArrayValue`, `DoubleArrayValue`,
`StringArrayValue`. `DeclareParameter` returns the effective value, which is
the command-line override if one was given. `node.GetParameterOr(name, def)`
avoids the error check for optional parameters.

Other operations: `SetParameter`, `SetParameters` (one result each),
`SetParametersAtomically`, `UndeclareParameter`, `DescribeParameter`,
`ListParameters(prefixes, depth)`, `HasParameter`. Every node exposes the
standard parameter services unless created with
`WithoutParameterServices()`, and publishes `/parameter_events` unless
created with `WithoutParameterEvents()`.

## Logging

rclpy:

```python
self.get_logger().info('x = %d' % x)
self.get_logger().set_level(LoggingSeverity.DEBUG)
```

goros:

```go
node.Logger().Info("x = " + strconv.Itoa(x))
node.Logger().Infof("x = %d", x)
node.Logger().SetLevel(goros.LogDebug)
child := node.Logger().Child("driver")   // named "<node>.driver"
```

Both forms write the rcutils console line and publish an
`rcl_interfaces/Log` on `/rosout`. Prefer the non-`f` forms in firmware to
keep `fmt` out of the binary.

## Time

```go
now := node.Now()                      // goros.Time == builtin_interfaces.Time
goTime := goros.TimeToGo(now)
stamp := goros.TimeFrom(time.Now())
d := goros.DurationFrom(1500 * time.Millisecond)

rate := goros.NewRate(10)              // 10 Hz loop helper
for ctx.OK() {
	...
	rate.Sleep()
}
```

To use another time source (a GPS-disciplined clock, a simulation) implement
`goros.Clock` and pass `goros.WithClock(c)` to `NewNode`.

## Graph introspection

```go
node.GetNodeNames()                     // []string of fully-qualified names
node.GetTopicNamesAndTypes()            // map[topic][]type
node.GetServiceNamesAndTypes()
node.CountPublishers("/chatter")
ctx.PublishersInfo("/chatter")          // node name, namespace, type, QoS, participant GUID
<-ctx.GraphChanged()                    // closed on the next discovery change
```

## Names and remapping

Names resolve as in ROS 2: `chatter` is relative to the node namespace,
`/abs` is absolute, `~/status` is private under the node, and `{node}` and
`{ns}` are substituted. `node.ResolveTopicName(name)` returns the final name
after remapping, useful in logs. Remapping comes from `--ros-args -r`:

```sh
talker --ros-args -r chatter:=/robot1/talk -r __node:=talker2 -r __ns:=/robot1
talker --ros-args -r talker:chatter:=talk      # only applies to node "talker"
```

## Tearing down

Entities have `Destroy()` methods. `node.Destroy()` cancels timers and
destroys all entities of the node, then removes it from the graph.
`ctx.Shutdown()` announces departure, destroys every node and closes the
transports. Calling `Publish` after shutdown returns `goros.ErrShutdown`.

## Writing a tool that handles any message type

Import `bitbucket.org/goros/msgs/all` for side effects and use the registry:

```go
m := msg.New("sensor_msgs/msg/Imu")       // nil if not compiled in
err := msg.Decode(payload, m)
req := msg.NewServiceRequest("example_interfaces/srv/AddTwoInts")
```

Generated structs carry `json` tags with the ROS field names, so
`encoding/json` round-trips them; this is what the CLI uses for `topic pub`
and `service call`.
