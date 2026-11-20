# ROS 2 to goros: concept and command mapping

goros implements the ROS 2 programming model, so the concept pages at
[docs.ros.org](https://docs.ros.org/en/jazzy/) are the primary documentation.
This page tells a ROS 2 reader what each concept is called in goros and where
behaviour differs. Links are pinned to the Jazzy release of the docs.

Rows marked **differs** describe a behavioural difference, not just a spelling
one. Rows marked **absent** describe ROS 2 features goros does not have; see
[status.md](status.md).

## Concepts

| ROS 2 concept | docs.ros.org | goros | Notes |
|---|---|---|---|
| Context / `rclcpp::init` | [About Nodes](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Nodes.html) | `goros.Init(opts...)`, `ctx.Shutdown()`, `ctx.OK()` | One `Context` is one participant in the graph. Several contexts in one process are allowed and see each other over the transport. |
| Node | [About Nodes](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Nodes.html) | `ctx.NewNode(name, goros.WithNamespace(ns))` | Node name and namespace validation follow the [names design doc](https://design.ros2.org/articles/topic_and_service_names.html). |
| Topic, publisher, subscription | [About Topics](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Topics.html) | `goros.NewPublisher[*T]`, `goros.NewSubscription(node, topic, qos, func(*T))` | Same topic with two different types is rejected with `ErrTypeMismatch`. |
| Service, client | [About Services](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Services.html) | `goros.NewService(node, name, handler)`, `goros.NewClient[Req, Resp](node, name)` | Handler returns `error`; a non-nil error is delivered to the caller as a failed call. **differs**: rclcpp has no error channel for services. |
| Action | [About Actions](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Actions.html) | **absent** | |
| Parameter | [About Parameters](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Parameters.html) | `node.DeclareParameter`, `GetParameter`, `SetParameter`, `AddOnSetParametersCallback` | Same declare-before-use rule, same `rcl_interfaces` types, same six `~/*_parameters` services. `allow_undeclared_parameters` is **absent**. |
| Interfaces (`.msg`, `.srv`) | [About Interfaces](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Interfaces.html) | `interfaces/<pkg>/msg/*.msg`, generated into `msgs/<pkg>` by `go generate ./msgs` | Same file format. `.action` files are **absent**. |
| Quality of Service | [About QoS](https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Quality-of-Service-Settings.html) | `goros.QoSProfile`, `QoSDefault`, `QoSSensorData`, `QoSServicesDefault`, `QoSParameters` | History, depth, reliability and durability are implemented with the ROS request/offer matching rules. Deadline, lifespan, liveliness are **absent**. Reliable delivery is real over RTPS (the host default); the goros-native UDP transport only matches on it. |
| Executor | [About Executors](https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Executors.html) | `goros.Spin(node)`, `NewSingleThreadedExecutor`, `NewMultiThreadedExecutor(n)` | Callback groups are **absent**; a multi-threaded executor may run two callbacks of the same node concurrently. |
| Timer | [About Executors](https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Executors.html) | `node.NewTimer(period, fn)`, `NewOneShotTimer` | Wall time only. |
| Time and clocks | [Clock and time design](https://design.ros2.org/articles/clock_and_time.html) | `node.Now()`, `goros.TimeFrom`, `goros.Clock` | System time only. `use_sim_time` and `/clock` are **absent**; a custom `Clock` can be injected with `goros.WithClock`. |
| Logging | [About Logging](https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Logging.html) | `node.Logger().Info(...)`, `Infof`, `SetLevel` | Same console format and severities; records go to `/rosout`. `RCUTILS_CONSOLE_OUTPUT_FORMAT` and the logging config file are **absent**. |
| Discovery | [About Discovery](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Discovery.html) | automatic; `ctx.NodeNames()`, `TopicNamesAndTypes()`, `GraphChanged()` | SPDP/SEDP over RTPS on hosts, so goros and DDS participants see each other. `ROS_STATIC_PEERS` has an equivalent in `rtps.Options.Peers`. |
| Domain ID | [About Domain ID](https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Domain-ID.html) | `ROS_DOMAIN_ID` or `goros.WithDomain(id)` | Same env var, same RTPS ports. |
| Middleware / rmw | [About middleware vendors](https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Different-Middleware-Vendors.html) | `transport.Transport` interface; `rtps` (default), `udp`, `serial`, `inproc` | **differs**: goros brings its own RTPS implementation rather than loading a vendor. `RMW_IMPLEMENTATION`, `ROS_LOCALHOST_ONLY` and DDS XML profiles have no effect. Services follow the rmw_fastrtps/rmw_connextdds convention, so they do not reach rmw_cyclonedds nodes (nor do Fast DDS services). |
| Node arguments | [Node arguments guide](https://docs.ros.org/en/jazzy/How-To-Guides/Node-arguments.html) | `goros.WithArgs(os.Args[1:])` | Supports `--ros-args -r from:=to`, `-r __node:=`, `-r __ns:=`, `node:topic:=x`, `-p name:=value`, `--log-level`, `--`. `--params-file` is **absent**. |
| Lifecycle (managed) nodes | [Managed nodes demo](https://docs.ros.org/en/jazzy/Tutorials/Demos/Managed-Nodes.html) | **absent** | |
| tf2 | [About tf2](https://docs.ros.org/en/jazzy/Concepts/Intermediate/About-Tf2.html) | **absent** | `geometry_msgs/TransformStamped` is generated, so publishing `/tf` by hand works; there is no buffer or listener. |
| Launch | [About Launch](https://docs.ros.org/en/jazzy/Concepts/Basic/About-Launch.html) | **absent** | Start processes with your shell, a Makefile, systemd, or a Go supervisor. |
| rosbag2 | [Recording and playback](https://docs.ros.org/en/jazzy/Tutorials/Beginner-CLI-Tools/Recording-And-Playing-Back-Data/Recording-And-Playing-Back-Data.html) | **absent** | `goros.NewRawSubscription` gives you CDR payloads if you want to write a recorder. |
| Build system (colcon, ament) | [Creating a package](https://docs.ros.org/en/jazzy/Tutorials/Beginner-Client-Libraries/Creating-Your-First-ROS2-Package.html) | `go build`, `go generate ./msgs` | No workspaces, overlays or `setup.bash`. A ROS "package" of interfaces is a directory under `interfaces/`. |

## Command line

`goros` mirrors `ros2`. The [CLI tutorials](https://docs.ros.org/en/jazzy/Tutorials/Beginner-CLI-Tools.html) apply with the substitutions below. Unlisted `ros2` verbs do not exist in goros.

| `ros2` | `goros` | Notes |
|---|---|---|
| `ros2 node list` | `goros node list` | Hidden nodes (any name token starting with `_`) are omitted, as in ROS 2. |
| `ros2 node info /n` | `goros node info /n` | Lists subscribers, publishers and service servers. Action servers and clients are not listed. |
| `ros2 topic list [-t]` | `goros topic list [-t] [-a]` | |
| `ros2 topic echo /t` | `goros topic echo /t [type] [--raw]` | Output is JSON rather than YAML. Type is discovered from publishers; pass it explicitly if nothing is publishing yet. |
| `ros2 topic pub [-r hz] [-1] /t type 'yaml'` | `goros topic pub [-r hz] [-1] /t type 'json'` | **differs**: message body is JSON with ROS field names. |
| `ros2 topic hz /t` | `goros topic hz /t` | |
| `ros2 topic info -v /t` | `goros topic info -v /t` | |
| `ros2 service list [-t]` | `goros service list [-t]` | |
| `ros2 service call /s type 'yaml'` | `goros service call /s type 'json'` | |
| `ros2 param list /n` | `goros param list /n` | |
| `ros2 param get /n p` | `goros param get /n p` | |
| `ros2 param set /n p value` | `goros param set /n p value` | Value syntax is the same YAML-flavoured scalar or `[a, b]` list. |
| `ros2 param describe /n p` | `goros param describe /n p` | |
| `ros2 param dump`, `load`, `delete` | none | |
| `ros2 interface list` | `goros interface list` | Lists types compiled into the binary. |
| `ros2 interface show type` | `goros interface show type` | Reads from `interfaces/`; set `GOROS_INTERFACES` if running elsewhere. |
| `ros2 run pkg exe` | run the binary | |
| `ros2 launch`, `ros2 bag`, `ros2 action`, `ros2 lifecycle`, `ros2 doctor`, `ros2 component` | none | |
| micro-ROS agent | `goros bridge --serial /dev/ttyACM0` | Forwards frames between a serial-attached board and the RTPS domain, so the board shows up in `ros2 node list`. |

## Behavioural differences worth knowing

- **DDS interoperability is by RTPS, not by linking a vendor.** `ros2 topic list` shows goros topics and `goros topic list` shows rclcpp nodes, verified against Cyclone DDS and not yet against Fast DDS; see [status.md](status.md) for the exact coverage. The two `goros` and `ros2` CLIs are interchangeable for the verbs listed above.
- **Service handler errors** reach goros clients as a Go `error`. In rclcpp a handler cannot fail; it must encode failure in the response type. A DDS client of a goros service that returns an error gets no reply at all, so services meant for ROS 2 clients should encode failure in the response.
- **Subscription callbacks receive a fresh message per sample.** There is no loaned or shared-pointer message; the callback owns it.
- **Blocking service calls inside callbacks** deadlock on a single-threaded executor, exactly as in rclcpp. Use `CallAsync` or a multi-threaded executor.
- **Publish with no matched remote subscribers sends nothing on the wire**, matching DDS behaviour. Subscribers announce themselves immediately, so matching takes milliseconds, but the first sample after a subscriber appears may be missed unless the publisher is transient-local.
- **Discovery period** defaults to one second (`goros.WithHelloPeriod` changes it). A participant that disappears without sending its goodbye is dropped after three periods or three seconds, whichever is longer.
- **QoS depth** bounds a per-subscription queue drained by the executor. If callbacks are slow, old samples are dropped and counted in `Subscription.Dropped()`. The node's callback queue (default 256) is a second bound; overflow there is counted in `Node.Dropped()`.
- **Transient-local replay** is sent to each newly discovered matching subscription. With `KeepLast` depth 1 this is the classic "latched topic".
- **Parameter type coercion**: an integer override for a double parameter is accepted and converted, matching `rclcpp` behaviour for command-line overrides. Any other mismatch is rejected unless the descriptor sets `DynamicTyping`.
- **Logger names** use the fully-qualified node name with slashes turned into dots, so `/robot1/listener` logs as `robot1.listener`, as rclcpp does.
