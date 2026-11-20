# Transports and TinyGo

ROS 2 delegates discovery and transport to a DDS implementation chosen by
`RMW_IMPLEMENTATION`. goros keeps the same split one level down: a
`Context` owns one or more `transport.Transport` values and hands them
small frames carrying discovery announcements, topic samples and service
traffic. Most transports move those frames verbatim, which is what makes
the library fit on a microcontroller. The `rtps` transport instead
translates them to and from DDS-RTPS on the wire, so on a host a goros
participant is an ordinary member of the ROS 2 graph. It is the default
there.

## Frames

Every frame is `'G' 'R' version kind` followed by a CDR body that starts
with the sender's 16-byte GUID. Kinds:

| Kind | Purpose | Body |
|---|---|---|
| Hello | periodic announcement | domain, every node of the participant with its publishers, subscriptions, services and clients, each with name, type and QoS |
| Bye | orderly departure | nothing |
| Data | topic sample | topic, type, CDR payload |
| Request | service call | service, type, sequence number, CDR payload |
| Response | service reply | service, target GUID, sequence number, ok flag, error text, CDR payload |

Topic and service payloads are standard CDR with the 4-byte encapsulation
header, identical to what DDS carries for the same type. Only the envelope
is goros-specific; the RTPS transport copies the payload bytes straight
into RTPS DATA submessages. A sixth kind, Data-replay, carries
transient-local history re-sent for a late joiner; frame-forwarding
transports deliver it as data, RTPS drops it because its writers keep that
history themselves.

Hello frames go out every second by default (`goros.WithHelloPeriod`) and
immediately after any endpoint is created or destroyed. A remote participant
not heard from for three periods, or three seconds, whichever is longer, is
dropped from the graph. Data frames are sent only while at least one remote
subscription matches the topic and type.

## Built-in transports

### DDS-RTPS (hosts, the default)

`goros.Init()` with no options opens an RTPS participant for
`ROS_DOMAIN_ID` (or 0):

```go
t, err := rtps.New(rtps.Options{Domain: 0})
ctx, _ := goros.Init(goros.WithTransport(t), goros.WithoutDefaultTransport())
```

What it implements, in RTPS terms:

- SPDP on `239.255.0.1:7400+250*domain`, announced every second with a
  12 s lease, plus unicast announcements to every known peer and to
  `Options.Peers`. SEDP over reliable, transient-local builtin writers.
- One unicast socket on the first free metatraffic port
  (`7410+250*domain+2*id`) for both discovery and user data. Only the
  preferred interface's address is advertised (`Options.Advertise`
  overrides; `Options.Interface` picks the interface). Multicast is used
  for SPDP only, which is also what Cyclone DDS does on macOS.
- Writers: reliable or best-effort, volatile or transient-local, bounded
  history, HEARTBEAT after every sample and once a second while a reader
  lags, retransmission and GAP on ACKNACK, DATA_FRAG above 60 KB.
- Readers: ACKNACK on heartbeat with the missing set, history fetch for
  transient-local readers, duplicate suppression, DATA_FRAG reassembly.
- ROS 2 conventions: `rt/` topics, `rq/`…`Request` and `rr/`…`Reply`
  service topics, `pkg::msg::dds_::Name_` type names, XCDR1 data
  representation, the `ros_discovery_info` topic in both directions so node
  names appear in `ros2 node list` and DDS node names appear in
  `goros node list`.
- Services correlate requests the way rmw_fastrtps and rmw_connextdds do:
  the reply carries the request's writer GUID and sequence number in
  `PID_RELATED_SAMPLE_IDENTITY`, and is unicast to the client's reply
  reader when the request named it.

One RTPS participant hosts every goros participant that announces through
it. For a plain `Context` that is one; for `goros bridge` it is every
board behind the bridge, which is exactly the micro-ROS agent model.

Verified against Cyclone DDS 0.10 in both directions (see
[status.md](status.md)); `tools/cyclone_interop/interop.py` is the script.
To check against a ROS 2 install:

```sh
go run ./examples/rtps_interop            # talker, latched topic, listener, service
ros2 topic echo /chatter                  # in another shell, same ROS_DOMAIN_ID
ros2 node list                            # shows /goros_interop
ros2 service call /add_two_ints example_interfaces/srv/AddTwoInts '{a: 2, b: 40}'
```

`Options.Debug` receives a line per discovery event and, with it set, a
line per packet; it is the first thing to turn on when a peer is silent.

### UDP multicast (hosts, goros-native)

The goros-native alternative: frames go out verbatim, so only goros
participants understand them. It is cheaper than RTPS and has no
discovery chatter beyond the Hello frames, which makes it the right choice
for bridges and goros-only meshes.

```go
t, err := udp.New(udp.Options{Domain: 0})
ctx, _ := goros.Init(goros.WithTransport(t), goros.WithoutDefaultTransport())
```

- Group `239.255.0.1`, port `7400 + 250*domain + 5`. The offset keeps it off
  the DDS ports for the same domain, so a goros graph and a DDS graph on
  the same machine ignore each other rather than logging garbage.
- The interface is chosen automatically: the first up, multicast-capable
  interface with an IPv4 address, preferring real NICs over loopback.
  Override with `Options.Interface`.
- Multicast loopback is enabled, so several processes on one host see each
  other. Go turns this off by default; goros turns it back on.
- `Options.Peers` adds unicast destinations for networks that drop
  multicast, such as some Wi-Fi access points and most VPNs.
- One sample is one datagram, so a message must serialize to under about
  64 KB. There is no fragmentation. `sensor_msgs/Image` at camera
  resolutions does not fit; downscale, compress, or use a serial/TCP style
  transport that you implement over a stream.
- Delivery is best-effort regardless of the QoS reliability setting. The
  reliability policy is used for matching only.

### Serial (microcontrollers and anything with a byte stream)

```go
link := serial.New(rw, maxFrame)   // rw is any io.ReadWriter
```

Frames are COBS-encoded with a CRC-16/CCITT and a zero delimiter, so the
receiver can resynchronise after noise or a mid-frame reset. Any
`io.ReadWriter` works: a TinyGo `machine.UART`, USB CDC via
`machine.Serial`, a `*os.File` for `/dev/ttyACM0`, a TCP connection, an
`io.Pipe` in tests.

The serial transport is point-to-point. To make the board visible to the
rest of the graph, run the bridge on the host:

```sh
stty -f /dev/tty.usbmodem1101 115200 raw     # macOS; Linux: stty -F /dev/ttyACM0 115200 raw
goros bridge --serial /dev/tty.usbmodem1101
```

`goros bridge` opens the device, joins the RTPS domain and copies frames in
both directions, so the board's node, topics and services appear to `ros2`
and to every goros process. Multiple `--serial` flags bridge several
boards. `--udp` adds the goros-native multicast transport, `--udp-listen`
the goros-native unicast one, `--no-rtps` drops RTPS, and `--peer HOST`
names static peers for networks without multicast. Baud rate is set with
`stty` because goros does not depend on a termios package; it treats the
device as a plain file.

The TinyGo serial port types implement `ReadByte` and `Buffered` rather than
`io.Reader`; wrap them with `serial.Adapt`:

```go
link := serial.New(serial.Adapt(machine.Serial), 4096)
```

The second argument bounds the largest frame the receiver will buffer.
Size it to your largest message plus a few hundred bytes; it is allocated
lazily as frames grow.

### Unicast UDP (TinyGo boards with networking, NAT-free LANs)

TinyGo's `net` package has no multicast and only works with a network
driver (Pico W, ESP32, Wio Terminal and others through
`tinygo.org/x/drivers/netlink`). On those boards there are two options.

Speak RTPS directly and skip the bridge:

```go
t, err := rtps.New(rtps.Options{
	Domain:    0,
	Peers:     []string{"192.168.1.10"},            // the ROS 2 machine(s)
	Advertise: []net.IP{net.IPv4(192, 168, 1, 50)}, // this board's address
})
```

`Peers` receive unicast participant announcements on the SPDP port and the
first four metatraffic unicast ports of the domain, the same thing
`ROS_STATIC_PEERS` does on the ROS 2 side; set `ROS_STATIC_PEERS` to the
board's address there too if the host must find the board first. This
build has been compiled for `pico-w` (about 50 KB of flash for the
transport and a talker) but not yet exercised on hardware.

Or use the lighter goros-native unicast transport against a host bridge:

```go
t, err := udp.New(udp.Options{Domain: 0, Peers: []string{"192.168.1.10:7405"}})
```

with `goros bridge --udp-listen :7405` on the host, or any participant
with `udp.NewUnicast(":7405", nil)`. Unicast endpoints learn the address
of anyone who sends to them, so only the board needs to know the host.

### In-process bus (tests, composition)

```go
bus := inproc.NewBus(256)
a, _ := goros.Init(goros.WithTransport(bus.Join()), goros.WithoutDefaultTransport())
b, _ := goros.Init(goros.WithTransport(bus.Join()), goros.WithoutDefaultTransport())
```

Nodes within one `Context` already talk to each other directly with no
transport at all. The bus is for several contexts in one process.

### Writing a transport

Implement four methods:

```go
type Transport interface {
	Send(buf []byte) error     // broadcast; must not retain buf
	Recv() ([]byte, error)     // block; return ErrClosed after Close
	Close() error              // must unblock Recv
	Name() string
}
```

Then pass it with `goros.WithTransport`. A TCP mesh, a CAN bus with
fragmentation, LoRa or a WebSocket to a browser all fit this shape, and so
does a translating transport like `rtps`, which keeps its own state and
synthesises Hello, Data, Request and Response frames from what it learns
on the wire. The bridge works with any of them.

## Building for TinyGo

The core library, the generated message packages and the serial, udp,
inproc and rtps transports compile under TinyGo 0.44 for host and
bare-metal targets (rtps in unicast mode, see above). The `cmd/goros` CLI
and the host examples use `encoding/json`, `os/signal` and multicast and
are host-only.

Measured firmware size for `examples/tinygo/pico_sensor`, which runs a
node with two publishers, one service and two timers:

| Target | Flash | Static RAM |
|---|---|---|
| pico, pico-w | 77 KB | 7.4 KB |
| esp32-coreboard-v2 | 84 KB | 6.3 KB |
| feather-nrf52840 | 123 KB | 6.8 KB |

Heap use is dominated by message allocation per publish and per received
sample, plus the serial receive buffer.

Rules of thumb:

1. **No default transport under TinyGo.** `goros.Init()` succeeds but is
   deaf until you add one with `goros.WithTransport`.
2. **Silence the console** with `goros.WithLogWriter(nil)` when the USB
   serial port carries goros frames; otherwise log lines corrupt the
   stream. Log through `/rosout` instead, which the host sees as a topic.
3. **Trim the node**: `goros.WithoutRosout()` saves a publisher and the
   `rcl_interfaces/Log` encoder; `goros.WithoutParameterServices()` removes
   six services and their request types; `goros.WithoutParameterEvents()`
   removes the `/parameter_events` publisher. Together they cut several
   kilobytes and shorten every Hello frame.
4. **Avoid `fmt`.** Use `Logger.Info` with `strconv` rather than `Infof`.
   `fmt` alone adds tens of kilobytes on Cortex-M0.
5. **Reuse message structs** in timers to avoid per-tick garbage. `Publish`
   serializes immediately and does not keep the pointer.
6. **GUIDs** come from `crypto/rand` where the target has a hardware RNG
   (RP2040, nRF52, ESP32 do). Elsewhere a clock-seeded fallback is used;
   two boards booting at the same instant could collide, so expose a
   hardware RNG or set the GUID yourself if that matters.
7. **Goroutines are fine.** Each timer and each transport receive loop is a
   goroutine; TinyGo's cooperative scheduler handles them. Callbacks still
   run on the executor goroutine, so `goros.Spin(node)` is your main loop.
8. **Blocking calls block the scheduler point, not the chip.** `time.Sleep`
   and channel operations yield; a busy loop does not.

Building and flashing:

```sh
tinygo build -target pico -size short -o pico_sensor.uf2 ./examples/tinygo/pico_sensor
tinygo flash -target pico ./examples/tinygo/pico_sensor
```

Board-specific code lives behind build tags, as the example does for the
RP2040 temperature sensor (`temp_rp2040.go` vs `temp_other.go`). The
`baremetal` tag keeps the example out of `go build ./...` and `go vet`.

Installing TinyGo without Homebrew: download the release tarball for your
platform from <https://github.com/tinygo-org/tinygo/releases>, extract it,
and put `tinygo/bin` on `PATH`; the macOS build needs the Xcode command line
tools. `make pico TINYGO=/path/to/tinygo/bin/tinygo` works without touching
`PATH`.
