// parameters declares a few parameters, reacts to changes and republishes
// its state. Try:
//
//	parameters --ros-args -p rate:=5 -p greeting:="hi there"
//	goros param list /param_demo
//	goros param set /param_demo greeting hello
package main

import (
	"os"
	"strconv"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/std_msgs"
)

func main() {
	ctx, err := goros.Init(goros.WithArgs(os.Args[1:]))
	if err != nil {
		panic(err)
	}
	defer ctx.Shutdown()

	node, _ := ctx.NewNode("param_demo")
	rate, _ := node.DeclareParameter("rate", goros.DoubleValue(1), goros.ParameterDescriptor{Description: "publish rate in Hz"})
	node.DeclareParameter("greeting", goros.StringValue("hello"))
	node.DeclareParameter("version", goros.IntegerValue(1), goros.ParameterDescriptor{ReadOnly: true})

	node.AddOnSetParametersCallback(func(ps []goros.Parameter) goros.SetParametersResult {
		for _, p := range ps {
			if p.Name == "rate" && (p.Value.Double <= 0 || p.Value.Double > 100) {
				return goros.SetParametersResult{Successful: false, Reason: "rate must be in (0, 100]"}
			}
		}
		return goros.SetParametersResult{Successful: true}
	})

	pub, _ := goros.NewPublisher[*std_msgs.String](node, "~/greeting", goros.QoSDefault)
	var timer *goros.Timer
	period := func() time.Duration {
		r := node.GetParameterOr("rate", goros.DoubleValue(1)).Double
		return time.Duration(float64(time.Second) / r)
	}
	tick := func() {
		g, _ := node.GetParameter("greeting")
		pub.Publish(&std_msgs.String{Data: g.String})
		node.Logger().Info(g.String + " (rate " + strconv.FormatFloat(rate.Double, 'f', -1, 64) + " Hz)")
		if p := period(); p != timer.Period() {
			timer.Cancel()
			timer = node.NewTimer(p, nil)
		}
	}
	timer = node.NewTimer(period(), func() { tick() })
	// Recreate the timer whenever rate changes.
	node.AddOnSetParametersCallback(func(ps []goros.Parameter) goros.SetParametersResult {
		for _, p := range ps {
			if p.Name == "rate" {
				rate = p.Value
				timer.Cancel()
				timer = node.NewTimer(time.Duration(float64(time.Second)/p.Value.Double), func() { tick() })
			}
		}
		return goros.SetParametersResult{Successful: true}
	})
	goros.Spin(node)
}
