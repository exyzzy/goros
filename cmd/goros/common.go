package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"bitbucket.org/goros"
	"bitbucket.org/goros/msg"
	"bitbucket.org/goros/names"
)

// discoveryWait is how long list-style commands wait for announcements.
// Remote participants announce every second by default.
const discoveryWait = 1200 * time.Millisecond

// newCLIContext starts a quiet participant with a hidden node.
func newCLIContext(nodeName string) (*goros.Context, *goros.Node, error) {
	ctx, err := goros.Init(goros.WithLogWriter(io.Discard), goros.WithHelloPeriod(250*time.Millisecond), goros.WithArgs(os.Args[1:]))
	if err != nil {
		return nil, nil, err
	}
	node, err := ctx.NewNode("_goros_cli_"+nodeName, goros.WithoutRosout(), goros.WithoutParameterServices(), goros.WithoutParameterEvents())
	if err != nil {
		ctx.Shutdown()
		return nil, nil, err
	}
	return ctx, node, nil
}

// waitDiscovery sleeps for a discovery round, returning early on Ctrl-C.
func waitDiscovery(ctx *goros.Context) {
	select {
	case <-time.After(discoveryWait):
	case <-ctx.Done():
	}
}

// onInterrupt shuts the context down on SIGINT so Spin returns.
func onInterrupt(ctx *goros.Context) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		ctx.Shutdown()
	}()
}

// hidden reports whether a name should be hidden from listings.
func hidden(name string) bool { return names.IsHidden(name) }

func sortedKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func toJSON(m msg.Message) string {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func fromJSON(text string, m msg.Message) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	return json.Unmarshal([]byte(text), m)
}

// popFlag removes "-x" style flags from args, reporting whether each was
// present; flags with values are returned via vals.
func parseFlags(args []string, bools []string, valued []string) (rest []string, set map[string]bool, vals map[string]string, err error) {
	set = map[string]bool{}
	vals = map[string]string{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		matched := false
		for _, b := range bools {
			if a == b {
				set[b] = true
				matched = true
			}
		}
		if matched {
			continue
		}
		for _, v := range valued {
			if a == v {
				if i+1 >= len(args) {
					return nil, nil, nil, errors.New(v + " requires a value")
				}
				vals[v] = args[i+1]
				i++
				matched = true
			} else if strings.HasPrefix(a, v+"=") {
				vals[v] = a[len(v)+1:]
				matched = true
			}
		}
		if !matched {
			if strings.HasPrefix(a, "--") {
				return nil, nil, nil, errors.New("unknown flag " + a)
			}
			rest = append(rest, a)
		}
	}
	return rest, set, vals, nil
}
