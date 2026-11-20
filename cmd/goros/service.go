package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msg"
)

func cmdService(args []string) error {
	if len(args) == 0 {
		return errors.New("service: expected list or call")
	}
	switch args[0] {
	case "list":
		_, set, _, err := parseFlags(args[1:], []string{"-t", "--show-types"}, nil)
		if err != nil {
			return err
		}
		ctx, node, err := newCLIContext("service")
		if err != nil {
			return err
		}
		defer ctx.Shutdown()
		waitDiscovery(ctx)
		services := node.GetServiceNamesAndTypes()
		for _, s := range sortedKeys(services) {
			if hidden(s) {
				continue
			}
			if set["-t"] || set["--show-types"] {
				fmt.Printf("%s [%s]\n", s, strings.Join(services[s], ", "))
			} else {
				fmt.Println(s)
			}
		}
		return nil
	case "call":
		return serviceCall(args[1:])
	}
	return errors.New("service: unknown subcommand " + args[0])
}

func serviceCall(args []string) error {
	if len(args) < 2 {
		return errors.New("service call: expected <service> <type> [json]")
	}
	name, typ := args[0], args[1]
	text := strings.Join(args[2:], " ")
	req := msg.NewServiceRequest(typ)
	if req == nil {
		return errors.New("unknown service type " + typ)
	}
	if err := fromJSON(text, req); err != nil {
		return err
	}
	ctx, node, err := newCLIContext("call")
	if err != nil {
		return err
	}
	defer ctx.Shutdown()
	onInterrupt(ctx)
	cli, err := goros.NewRawClient(node, name, typ)
	if err != nil {
		return err
	}
	fmt.Println("waiting for service to become available...")
	if !cli.WaitForService(10 * time.Second) {
		return errors.New("service " + name + " not available")
	}
	fmt.Println("requester: making request:", typ)
	fmt.Println(toJSON(req))
	fmt.Println()
	cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	payload, err := cli.CallRaw(cctx, msg.Encode(req))
	if err != nil {
		return err
	}
	resp := msg.NewServiceResponse(typ)
	if err := msg.Decode(payload, resp); err != nil {
		return err
	}
	fmt.Println("response:")
	fmt.Println(toJSON(resp))
	return nil
}
