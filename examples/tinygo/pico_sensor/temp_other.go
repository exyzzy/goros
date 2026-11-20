//go:build baremetal && !rp2040

package main

// readTemperature is a placeholder on boards without an on-die sensor;
// wire up a driver from tinygo.org/x/drivers here.
func readTemperature() float64 { return 25 }
