package goros

import (
	"time"

	"bitbucket.org/goros/msgs/builtin_interfaces"
)

// Time is the ROS time representation used in message headers.
type Time = builtin_interfaces.Time

// Duration is the ROS duration representation.
type Duration = builtin_interfaces.Duration

// TimeFrom converts a Go time to ROS time.
func TimeFrom(t time.Time) Time {
	ns := t.UnixNano()
	return Time{Sec: int32(ns / 1e9), Nanosec: uint32(ns % 1e9)}
}

// TimeToGo converts ROS time to Go time.
func TimeToGo(t Time) time.Time { return time.Unix(int64(t.Sec), int64(t.Nanosec)) }

// DurationFrom converts a Go duration to a ROS duration.
func DurationFrom(d time.Duration) Duration {
	return Duration{Sec: int32(d / time.Second), Nanosec: uint32(d % time.Second)}
}

// DurationToGo converts a ROS duration to a Go duration.
func DurationToGo(d Duration) time.Duration {
	return time.Duration(d.Sec)*time.Second + time.Duration(d.Nanosec)
}

// Clock is a source of ROS time. ROS 2 distinguishes system, steady and ROS
// (simulated) time; goros ships a system clock and lets you plug in others.
type Clock interface {
	Now() Time
}

// SystemClock reads the wall clock.
type SystemClock struct{}

func (SystemClock) Now() Time { return TimeFrom(time.Now()) }

// Rate helps loops run at a fixed frequency, like rclcpp::Rate.
type Rate struct {
	period time.Duration
	next   time.Time
}

// NewRate creates a Rate for the given frequency in Hz.
func NewRate(hz float64) *Rate {
	return &Rate{period: time.Duration(float64(time.Second) / hz), next: time.Now()}
}

// Sleep blocks until the next period boundary.
func (r *Rate) Sleep() {
	r.next = r.next.Add(r.period)
	if d := time.Until(r.next); d > 0 {
		time.Sleep(d)
	} else {
		r.next = time.Now()
	}
}

// Period returns the loop period.
func (r *Rate) Period() time.Duration { return r.period }
