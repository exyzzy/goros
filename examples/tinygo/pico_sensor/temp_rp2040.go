//go:build rp2040

package main

import "machine"

// readTemperature returns the die temperature in degrees Celsius.
func readTemperature() float64 { return float64(machine.ReadTemperature()) / 1000 }
