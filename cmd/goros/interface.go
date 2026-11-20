package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"bitbucket.org/goros/msg"
)

func cmdInterface(args []string) error {
	if len(args) == 0 {
		return errors.New("interface: expected list or show")
	}
	switch args[0] {
	case "list":
		names := append(msg.TypeNames(), msg.ServiceTypeNames()...)
		sort.Strings(names)
		fmt.Println("Messages and services:")
		for _, n := range names {
			fmt.Println("    " + n)
		}
		return nil
	case "show":
		if len(args) < 2 {
			return errors.New("interface show: expected <type>")
		}
		return interfaceShow(args[1])
	}
	return errors.New("interface: unknown subcommand " + args[0])
}

// interfaceShow prints the .msg/.srv definition. Definitions are looked up
// relative to GOROS_INTERFACES or the source tree next to the binary.
func interfaceShow(typ string) error {
	parts := strings.Split(typ, "/")
	if len(parts) != 3 {
		return errors.New("expected <pkg>/<msg|srv>/<Name>")
	}
	ext := ".msg"
	if parts[1] == "srv" {
		ext = ".srv"
	}
	candidates := []string{os.Getenv("GOROS_INTERFACES"), "interfaces", filepath.Join("..", "interfaces")}
	if exe, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(exe), "..", "interfaces"), filepath.Join(filepath.Dir(exe), "..", "..", "interfaces"))
	}
	for _, root := range candidates {
		if root == "" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, parts[0], parts[1], parts[2]+ext))
		if err == nil {
			fmt.Print(string(data))
			return nil
		}
	}
	if m := msg.New(typ); m != nil {
		fmt.Println("# definition file not found; type is registered with fields:")
		fmt.Println(toJSON(m))
		return nil
	}
	return errors.New("unknown interface " + typ)
}
