// imu_serial_host shows the host side of a serial link: it attaches a
// serial transport on top of a device file (or any io.ReadWriter) in
// addition to UDP, so a microcontroller running examples/tinygo/pico_sensor
// appears in the ROS graph. `goros bridge` does the same thing generically.
//
//	stty -f /dev/tty.usbmodem1101 115200 raw
//	imu_serial_host /dev/tty.usbmodem1101
package main

import (
	"fmt"
	"os"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/sensor_msgs"
	"bitbucket.org/goros/transport/serial"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: imu_serial_host <serial device>")
		os.Exit(2)
	}
	f, err := os.OpenFile(os.Args[1], os.O_RDWR, 0)
	if err != nil {
		panic(err)
	}
	ctx, err := goros.Init(goros.WithTransport(serial.New(f, 0)))
	if err != nil {
		panic(err)
	}
	defer ctx.Shutdown()
	node, _ := ctx.NewNode("imu_host")
	goros.NewSubscription(node, "/imu/data_raw", goros.QoSSensorData, func(m *sensor_msgs.Imu) {
		node.Logger().Infof("accel=(%.2f %.2f %.2f) gyro=(%.2f %.2f %.2f)",
			m.LinearAcceleration.X, m.LinearAcceleration.Y, m.LinearAcceleration.Z,
			m.AngularVelocity.X, m.AngularVelocity.Y, m.AngularVelocity.Z)
	})
	goros.Spin(node)
}
