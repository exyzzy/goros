//go:build baremetal

// pico_sensor is a goros node running on a microcontroller. It publishes
// sensor_msgs/Imu and sensor_msgs/Temperature over the USB serial port
// and offers a std_srvs/SetBool service that toggles the on-board LED.
//
// Build and flash (Raspberry Pi Pico shown; any TinyGo board with a serial
// port works):
//
//	tinygo flash -target pico ./examples/tinygo/pico_sensor
//
// Then on the host:
//
//	stty -f /dev/tty.usbmodem1101 115200 raw
//	goros bridge --serial /dev/tty.usbmodem1101
//	goros topic echo /imu/data_raw
//	goros service call /led std_srvs/srv/SetBool '{"data": true}'
package main

import (
	"machine"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/sensor_msgs"
	"bitbucket.org/goros/msgs/std_srvs"
	"bitbucket.org/goros/transport/serial"
)

func main() {
	led := machine.LED
	led.Configure(machine.PinConfig{Mode: machine.PinOutput})

	// The USB CDC port carries goros frames, so console logging is disabled.
	link := serial.New(serial.Adapt(machine.Serial), 4096)
	ctx, err := goros.Init(goros.WithTransport(link), goros.WithLogWriter(nil))
	if err != nil {
		for {
			led.High()
			time.Sleep(100 * time.Millisecond)
			led.Low()
			time.Sleep(100 * time.Millisecond)
		}
	}
	node, _ := ctx.NewNode("pico", goros.WithoutRosout())

	imuPub, _ := goros.NewPublisher[*sensor_msgs.Imu](node, "/imu/data_raw", goros.QoSSensorData)
	tempPub, _ := goros.NewPublisher[*sensor_msgs.Temperature](node, "/temperature", goros.QoSSensorData)

	goros.NewService(node, "/led", func(req *std_srvs.SetBool_Request, resp *std_srvs.SetBool_Response) error {
		led.Set(req.Data)
		resp.Success = true
		resp.Message = "led toggled"
		return nil
	})

	imu := sensor_msgs.NewImu()
	imu.Header.FrameId = "imu_link"
	var phase float64
	node.NewTimer(20*time.Millisecond, func() {
		phase += 0.02
		imu.Header.Stamp = node.Now()
		// Replace with real driver reads (tinygo.org/x/drivers/mpu6050 etc).
		imu.LinearAcceleration.X = 0.1 * phase
		imu.LinearAcceleration.Z = 9.81
		imu.AngularVelocity.Z = 0.5
		imuPub.Publish(imu)
	})
	node.NewTimer(time.Second, func() {
		t := sensor_msgs.NewTemperature()
		t.Header.Stamp = node.Now()
		t.Temperature = readTemperature()
		tempPub.Publish(t)
	})
	goros.Spin(node)
}
