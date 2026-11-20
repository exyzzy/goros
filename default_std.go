//go:build !tinygo

package goros

import (
	"bitbucket.org/goros/transport"
	"bitbucket.org/goros/transport/rtps"
)

// defaultTransport returns DDS-RTPS on hosts, so goros participants join
// the ROS 2 graph of rclcpp/rclpy nodes directly. The lighter goros-native
// UDP transport (transport/udp) remains available through WithTransport.
func defaultTransport(domain int) (transport.Transport, error) {
	return rtps.New(rtps.Options{Domain: domain})
}
