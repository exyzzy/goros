# Status and roadmap

Last updated 2026-11-19 against TinyGo 0.44 and Go 1.27.

## Headline

goros implements the ROS 2 client-library model and the standard interface
packages, and on hosts it speaks DDS-RTPS natively, so goros nodes and
rclcpp/rclpy nodes see each other and the `ros2` command line works against
goros. Interoperability has been verified against Cyclone DDS 0.10 in both
directions (discovery, topics, latched topics, services from a DDS client);
Fast DDS, the Jazzy default, has not been exercised yet. Microcontrollers
join through a serial bridge or over unicast RTPS.

## Feature matrix

| Area | State | Notes |
|---|---|---|
| Context, nodes, namespaces | done | Name validation per the ROS 2 design doc |
| Publishers, subscriptions | done | Typed and raw variants |
| Services, clients | done | Sync with `context.Context`, async, `WaitForService`, error propagation |
| Timers | done | Wall time, periodic and one-shot |
| Executors | done | Single- and multi-threaded, `SpinOnce`, `SpinSome`, `SpinUntilFutureComplete` |
| Callback groups | not implemented | Multi-threaded executor may run a node's callbacks concurrently |
| QoS history, depth | done | Per-subscription queue, overflow counted |
| QoS reliability | done over RTPS | Heartbeat/ACKNACK/GAP retransmission from a bounded history; the goros-native UDP transport matches on it but is best-effort |
| QoS durability | done | Transient-local replay to local and remote late joiners |
| QoS deadline, lifespan, liveliness | not implemented | |
| Parameters | done | Declare/get/set/undeclare, descriptors, read-only, callbacks, overrides, events, six standard services |
| Parameter files (`--params-file`) | not implemented | Flag is accepted and ignored |
| Logging | done | rcutils console format, `/rosout`, per-logger levels, `--log-level` |
| Graph introspection | done | Nodes, topics, services, per-endpoint QoS, counts, change notification |
| Name remapping | done | `-r`, node-scoped rules, `__node`, `__ns`, substitutions |
| Time | system clock only | `use_sim_time`, `/clock`, steady clock not implemented; `Clock` is pluggable |
| Actions | not implemented | |
| Lifecycle nodes | not implemented | |
| tf2 | not implemented | Transform messages are generated |
| rosbag | not implemented | Raw subscriptions expose CDR payloads |
| Launch | not implemented | |
| Message generation | done | `.msg`/`.srv`; primitives, bounded types, arrays, sequences, nesting, constants, defaults |
| `.action`, `.idl` input | not implemented | |
| CDR serialization | done | XCDR1 little-endian out, both endiannesses in; byte-exact with rclcpp for generated types |
| DDS-RTPS transport | done | Default on hosts. SPDP/SEDP, reliable + best-effort, transient-local, DATA_FRAG, ROS 2 name mangling, `ros_discovery_info`, services (Fast DDS convention), static peers, TinyGo unicast build |
| UDP multicast transport (goros-native) | done | Hosts only; interface selection, loopback, unicast peers |
| Serial transport | done | COBS + CRC-16, resync, TinyGo adapter |
| Unicast UDP transport | done | Hosts and TinyGo with netdev; learns peers |
| Message fragmentation | RTPS only | DATA_FRAG send and reassembly (no NACK_FRAG, so a lost fragment loses the sample); goros-native UDP and serial carry one sample per datagram/frame, about 64 KB max |
| Bridge | done | Any-to-any frame forwarding; serial boards join the RTPS domain (micro-ROS agent role) |
| CLI | done | `node`, `topic`, `service`, `param`, `interface`, `bridge` |
| DDS-RTPS interoperability | done, Cyclone-verified | See below for what was tested and what was not |
| Security (SROS 2) | not implemented | No authentication or encryption on any transport |

Interface packages shipped: `builtin_interfaces`, `std_msgs`,
`geometry_msgs`, `sensor_msgs` (common subset), `nav_msgs`,
`rcl_interfaces`, `example_interfaces`, `std_srvs`. Adding more is a matter
of dropping the `.msg` files into `interfaces/` and running
`go generate ./msgs`.

## Test coverage

`go test -race ./...` runs 31 tests covering serialization, names, frames,
serial framing, intra-process delivery, discovery and delivery between
participants over the in-process bus and over real UDP multicast, QoS
matching and depth, transient-local replay, services in every mode,
timers, parameters including the services, remapping, `/rosout`, node
teardown, a serial-bridged participant, RTPS parameter-list and
submessage encoding, and two goros participants talking over real RTPS
(pub/sub with node names via `ros_discovery_info`, transient-local
replay, services). There is no upstream ROS 2 conformance suite to run;
`TestStdMsgsStringWireCompat` is the one test that asserts against bytes
produced by rclcpp.

Interoperability with a real DDS implementation is checked outside `go
test` with `tools/cyclone_interop/interop.py` (Cyclone DDS 0.10.5 through
its Python binding) against `examples/rtps_interop`, in both start-up
orders: Cyclone discovers every goros endpoint with the right topic and
type names, receives `/chatter` and the three latched `/latched` samples,
gets replies to `/add_two_ints` requests, and goros receives Cyclone's
topic, its latched history as a late joiner, and lists the Cyclone
participant in its graph.

TinyGo builds are verified for host (macOS arm64), `pico`, `pico-w`,
`esp32-coreboard-v2` and `feather-nrf52840`. The firmware example has been
compiled but not exercised on hardware in CI; the serial path is tested on
the host with pipes.

## Known limitations in more detail

**DDS interoperability: what is and is not covered.**

- Verified: Cyclone DDS 0.10 (see Test coverage). Not yet run: Fast DDS
  (the Jazzy/Humble default), Connext, and the `ros2` CLI itself. The
  conventions used (`rt/`, `rq/`, `rr/` prefixes, `pkg::msg::dds_::Name_`
  types, `ros_discovery_info` with 16-byte GIDs, XCDR1 payloads) are those
  of rmw_fastrtps and rmw_cyclonedds in Iron and later; Humble's 24-byte
  GIDs are parsed but not emitted.
- Services use the Fast DDS/Connext correlation: the request is identified
  by its RTPS writer GUID and sequence number and the reply carries
  `PID_RELATED_SAMPLE_IDENTITY` (0x0083 and the legacy 0x800f). Clients and
  servers under rmw_cyclonedds prepend a header to the payload instead and
  are not interoperable, as they are not with Fast DDS.
- Not implemented: XTypes type information (matching is by type name,
  which is what ROS 2 does anyway), DATA_FRAG negative acknowledgement,
  shared-memory transport, security, IPv6 locators, writer liveliness
  assertions (participant lease only), deadline/lifespan.
- One participant per transport hosts every goros Context that announces
  through it; `ros2 node info` attributes shared topics such as `/rosout`
  to the first node that declared them.
- Discovery traffic is unicast to each peer, and only one address, the
  preferred interface's, is advertised. Multi-homed robots should set
  `rtps.Options.Advertise`. Peers without multicast reach us through
  `rtps.Options.Peers` (`ROS_STATIC_PEERS` on the ROS 2 side).

**Reliability on the goros-native UDP transport.** A `Reliable` publisher
and subscriber match, but nothing retransmits lost datagrams there. Use
RTPS (the default) when reliability matters; the native transport exists
for bridges and boards.

**Large messages.** Over RTPS samples above 60 KB are fragmented and
reassembled, with no recovery of a lost fragment. Over the goros-native UDP
transport a sample must fit one datagram. The serial transport has no such
limit beyond the receiver's buffer.

**Single service instance per name per context.** Two nodes in one context
cannot serve the same service name. Across contexts, the first discovered
server receives the request.

**Executor fairness.** Each subscription drains all of its queued samples
when scheduled, so a very busy topic can delay callbacks of other entities
on the same node. Lower its depth or give it its own node.

**Clock.** `node.Now()` is wall time. There is no monotonic ROS clock and no
simulated time.

## Roadmap, in the order that unlocks the most

1. **Run the upstream suite.** With RTPS in place the `ros2` CLI and the
   `demo_nodes_cpp` talker/listener/add_two_ints are the integration tests
   goros should pass. Run them against a Fast DDS install (Docker
   `ros:jazzy` on Linux is enough) and fix whatever differs from Cyclone.
2. **Golden-byte serialization tests** recorded from a real ROS 2
   installation for every generated package.
3. **NACK_FRAG** so large samples survive a lost fragment, and
   **fragmentation** in the goros-native frame layer.
4. **Callback groups** and mutually-exclusive scheduling in the
   multi-threaded executor.
5. **Actions**, built on the existing service and topic primitives.
6. **`--params-file`** YAML loading.
7. **Steady and simulated clocks**, `use_sim_time`.
8. **Lifecycle nodes.**
9. **tf2** buffer and listener, the last piece most navigation stacks need.
