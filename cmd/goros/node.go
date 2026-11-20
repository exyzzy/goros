package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"bitbucket.org/goros"
	"bitbucket.org/goros/names"
)

func cmdNode(args []string) error {
	if len(args) == 0 {
		return errors.New("node: expected list or info")
	}
	ctx, node, err := newCLIContext("node")
	if err != nil {
		return err
	}
	defer ctx.Shutdown()
	switch args[0] {
	case "list":
		waitDiscovery(ctx)
		for _, n := range node.GetNodeNames() {
			if !hidden(n) {
				fmt.Println(n)
			}
		}
		return nil
	case "info":
		if len(args) < 2 {
			return errors.New("node info: expected node name")
		}
		waitDiscovery(ctx)
		return nodeInfo(ctx, args[1])
	}
	return errors.New("node: unknown subcommand " + args[0])
}

func nodeInfo(ctx *goros.Context, name string) error {
	found := false
	for _, n := range ctx.NodeNames() {
		if n == name {
			found = true
		}
	}
	if !found {
		return errors.New("unknown node " + name)
	}
	// The graph API is per-topic, so gather endpoints by scanning topics.
	var subs, pubs, services []string
	for topic := range ctx.TopicNamesAndTypes() {
		for _, e := range ctx.SubscriptionsInfo(topic) {
			if names.FullyQualifiedNodeName(e.NodeName, e.NodeNamespace) == name {
				subs = append(subs, "    "+topic+": "+e.TopicType)
			}
		}
		for _, e := range ctx.PublishersInfo(topic) {
			if names.FullyQualifiedNodeName(e.NodeName, e.NodeNamespace) == name {
				pubs = append(pubs, "    "+topic+": "+e.TopicType)
			}
		}
	}
	for s, types := range ctx.ServiceNamesAndTypes() {
		if strings.HasPrefix(s, name+"/") {
			services = append(services, "    "+s+": "+strings.Join(types, ", "))
		}
	}
	fmt.Println(name)
	fmt.Println("  Subscribers:")
	printSorted(subs)
	fmt.Println("  Publishers:")
	printSorted(pubs)
	fmt.Println("  Service Servers:")
	printSorted(services)
	return nil
}

func printSorted(lines []string) {
	sort.Strings(lines)
	for _, l := range lines {
		fmt.Println(l)
	}
}
