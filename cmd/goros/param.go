package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msgs/rcl_interfaces"
)

func cmdParam(args []string) error {
	if len(args) < 2 {
		return errors.New("param: expected list|get|set|describe <node> ...")
	}
	sub, nodeName := args[0], args[1]
	ctx, node, err := newCLIContext("param")
	if err != nil {
		return err
	}
	defer ctx.Shutdown()
	onInterrupt(ctx)
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ready := func(c interface{ WaitForService(time.Duration) bool }) error {
		if !c.WaitForService(5 * time.Second) {
			return errors.New("node " + nodeName + " does not offer parameter services")
		}
		return nil
	}
	switch sub {
	case "list":
		cli, _ := goros.NewClient[rcl_interfaces.ListParameters_Request, rcl_interfaces.ListParameters_Response](node, nodeName+"/list_parameters")
		if err := ready(cli); err != nil {
			return err
		}
		resp, err := cli.Call(cctx, &rcl_interfaces.ListParameters_Request{})
		if err != nil {
			return err
		}
		for _, n := range resp.Result.Names {
			fmt.Println("  " + n)
		}
		return nil
	case "get":
		if len(args) < 3 {
			return errors.New("param get: expected <node> <name>")
		}
		cli, _ := goros.NewClient[rcl_interfaces.GetParameters_Request, rcl_interfaces.GetParameters_Response](node, nodeName+"/get_parameters")
		if err := ready(cli); err != nil {
			return err
		}
		resp, err := cli.Call(cctx, &rcl_interfaces.GetParameters_Request{Names: []string{args[2]}})
		if err != nil {
			return err
		}
		if len(resp.Values) == 0 || resp.Values[0].Type == 0 {
			return errors.New("parameter not set")
		}
		v := goros.ParameterValueFromMsg(resp.Values[0])
		fmt.Printf("%s value is: %s\n", v.Type, v.AsString())
		return nil
	case "set":
		if len(args) < 4 {
			return errors.New("param set: expected <node> <name> <value>")
		}
		cli, _ := goros.NewClient[rcl_interfaces.SetParameters_Request, rcl_interfaces.SetParameters_Response](node, nodeName+"/set_parameters")
		if err := ready(cli); err != nil {
			return err
		}
		v := goros.ParseParameterValue(args[3])
		resp, err := cli.Call(cctx, &rcl_interfaces.SetParameters_Request{Parameters: []rcl_interfaces.Parameter{{Name: args[2], Value: v.ToMsg()}}})
		if err != nil {
			return err
		}
		if len(resp.Results) == 0 || !resp.Results[0].Successful {
			reason := "unknown"
			if len(resp.Results) > 0 {
				reason = resp.Results[0].Reason
			}
			return errors.New("setting parameter failed: " + reason)
		}
		fmt.Println("Set parameter successful")
		return nil
	case "describe":
		if len(args) < 3 {
			return errors.New("param describe: expected <node> <name>")
		}
		cli, _ := goros.NewClient[rcl_interfaces.DescribeParameters_Request, rcl_interfaces.DescribeParameters_Response](node, nodeName+"/describe_parameters")
		if err := ready(cli); err != nil {
			return err
		}
		resp, err := cli.Call(cctx, &rcl_interfaces.DescribeParameters_Request{Names: []string{args[2]}})
		if err != nil {
			return err
		}
		for _, d := range resp.Descriptors {
			fmt.Printf("Parameter name: %s\n  Type: %s\n  Description: %s\n  Constraints: %s\n  Read only: %v\n",
				d.Name, goros.ParameterType(d.Type), d.Description, d.AdditionalConstraints, d.ReadOnly)
		}
		return nil
	}
	return errors.New("param: unknown subcommand " + sub)
}
