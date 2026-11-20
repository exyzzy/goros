package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"bitbucket.org/goros/transport"
	"bitbucket.org/goros/transport/rtps"
	"bitbucket.org/goros/transport/serial"
	"bitbucket.org/goros/transport/udp"
)

// cmdBridge forwards frames between transports, typically a serial link to
// a microcontroller and the DDS-RTPS domain: the goros equivalent of the
// micro-ROS agent. Multiple --serial flags are allowed.
//
//	goros bridge --serial /dev/tty.usbmodem1101
//	goros bridge --serial /dev/ttyACM0 --udp-listen :7405 --peer 10.0.0.7:7405
//
// Flags: --serial DEV (repeatable), --udp-listen ADDR (goros-native unicast
// UDP for boards on Wi-Fi), --udp (goros-native multicast UDP), --no-rtps,
// --peer HOST[:PORT] (static peers for both UDP transports), --domain N.
func cmdBridge(args []string) error {
	var serials []string
	var peers []string
	udpListen := ""
	nativeUDP := false
	noRTPS := false
	domain := 0
	if v := os.Getenv("ROS_DOMAIN_ID"); v != "" {
		fmt.Sscanf(v, "%d", &domain)
	}
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--serial" && i+1 < len(args):
			i++
			serials = append(serials, args[i])
		case strings.HasPrefix(a, "--serial="):
			serials = append(serials, a[len("--serial="):])
		case a == "--peer" && i+1 < len(args):
			i++
			peers = append(peers, args[i])
		case a == "--udp-listen" && i+1 < len(args):
			i++
			udpListen = args[i]
		case a == "--udp":
			nativeUDP = true
		case a == "--no-rtps", a == "--no-multicast":
			noRTPS = true
		case a == "--domain" && i+1 < len(args):
			i++
			fmt.Sscanf(args[i], "%d", &domain)
		default:
			return errors.New("bridge: unknown argument " + a)
		}
	}
	if len(serials) == 0 && udpListen == "" {
		return errors.New("bridge: need at least one --serial device or --udp-listen")
	}
	var links []transport.Transport
	for _, dev := range serials {
		f, err := os.OpenFile(dev, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		links = append(links, serial.New(f, 0))
		fmt.Println("bridge: serial", dev, "(set the baud rate with stty first)")
	}
	if !noRTPS {
		t, err := rtps.New(rtps.Options{Domain: domain, Peers: peers})
		if err != nil {
			return err
		}
		links = append(links, t)
		fmt.Println("bridge:", t.Name())
	}
	if nativeUDP {
		var udpPeers []string
		for _, p := range peers {
			if _, _, err := net.SplitHostPort(p); err != nil {
				p = net.JoinHostPort(p, fmt.Sprint(udp.Port(domain)))
			}
			udpPeers = append(udpPeers, p)
		}
		t, err := udp.New(udp.Options{Domain: domain, Peers: udpPeers})
		if err != nil {
			return err
		}
		links = append(links, t)
		fmt.Println("bridge:", t.Name())
	}
	if udpListen != "" {
		t, err := udp.NewUnicast(udpListen, peers)
		if err != nil {
			return err
		}
		links = append(links, t)
		fmt.Println("bridge: unicast", udpListen)
	}
	if len(links) < 2 {
		return errors.New("bridge: need at least two transports to bridge")
	}
	return transport.Bridge(links...)
}
