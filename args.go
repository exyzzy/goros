package goros

import (
	"errors"
	"strconv"
	"strings"
)

// remapRule is a parsed "-r [node:]from:=to" rule.
type remapRule struct {
	node string // "" = applies to all nodes
	from string
	to   string
}

// parsedArgs holds everything recognised after --ros-args.
type parsedArgs struct {
	remaps   []remapRule
	params   []Parameter // -p overrides (node-qualified names allowed: "node:name")
	logLevel LogLevel
	hasLog   bool
}

// ParseArgs extracts ROS arguments from a command line, mirroring rcl:
//
//	prog --ros-args -r chatter:=talk -r __node:=foo -r __ns:=/robot1 \
//	     -p rate:=10 --log-level debug -- other args
//
// It returns the remaining (non-ROS) arguments.
func parseArgs(args []string) (parsedArgs, []string, error) {
	var pa parsedArgs
	var rest []string
	inRos := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !inRos {
			if a == "--ros-args" {
				inRos = true
				continue
			}
			rest = append(rest, a)
			continue
		}
		switch {
		case a == "--":
			inRos = false
		case a == "-r" || a == "--remap":
			if i+1 >= len(args) {
				return pa, nil, errors.New("goros: -r requires an argument")
			}
			i++
			r, err := parseRemap(args[i])
			if err != nil {
				return pa, nil, err
			}
			pa.remaps = append(pa.remaps, r)
		case a == "-p" || a == "--param":
			if i+1 >= len(args) {
				return pa, nil, errors.New("goros: -p requires an argument")
			}
			i++
			p, err := parseParamArg(args[i])
			if err != nil {
				return pa, nil, err
			}
			pa.params = append(pa.params, p)
		case a == "--log-level":
			if i+1 >= len(args) {
				return pa, nil, errors.New("goros: --log-level requires an argument")
			}
			i++
			lvl, ok := ParseLogLevel(args[i])
			if !ok {
				return pa, nil, errors.New("goros: unknown log level " + args[i])
			}
			pa.logLevel, pa.hasLog = lvl, true
		case a == "--params-file", a == "-e", a == "--enclave":
			i++ // not supported; skip the value
		default:
			return pa, nil, errors.New("goros: unknown ROS argument " + a)
		}
	}
	return pa, rest, nil
}

func parseRemap(s string) (remapRule, error) {
	i := strings.Index(s, ":=")
	if i < 0 {
		return remapRule{}, errors.New("goros: bad remap rule " + s)
	}
	r := remapRule{from: s[:i], to: s[i+2:]}
	if j := strings.IndexByte(r.from, ':'); j >= 0 {
		r.node, r.from = r.from[:j], r.from[j+1:]
	}
	if r.from == "" || r.to == "" {
		return r, errors.New("goros: bad remap rule " + s)
	}
	return r, nil
}

func parseParamArg(s string) (Parameter, error) {
	i := strings.Index(s, ":=")
	if i < 0 {
		return Parameter{}, errors.New("goros: bad parameter " + s)
	}
	return Parameter{Name: s[:i], Value: ParseParameterValue(s[i+2:])}, nil
}

// ParseParameterValue interprets a YAML-flavoured scalar or flow sequence
// as used on the ROS command line: true/false, integers, floats, quoted or
// bare strings, and [a, b, c] lists of those.
func ParseParameterValue(s string) ParameterValue {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		inner := strings.TrimSpace(s[1 : len(s)-1])
		if inner == "" {
			return StringArrayValue(nil)
		}
		parts := strings.Split(inner, ",")
		vals := make([]ParameterValue, len(parts))
		allBool, allInt, allNum := true, true, true
		for i, p := range parts {
			vals[i] = ParseParameterValue(p)
			switch vals[i].Type {
			case ParameterBool:
				allInt, allNum = false, false
			case ParameterInteger:
				allBool = false
			case ParameterDouble:
				allBool, allInt = false, false
			default:
				allBool, allInt, allNum = false, false, false
			}
		}
		switch {
		case allBool:
			out := make([]bool, len(vals))
			for i, v := range vals {
				out[i] = v.Bool
			}
			return BoolArrayValue(out)
		case allInt:
			out := make([]int64, len(vals))
			for i, v := range vals {
				out[i] = v.Integer
			}
			return IntegerArrayValue(out)
		case allNum:
			out := make([]float64, len(vals))
			for i, v := range vals {
				if v.Type == ParameterInteger {
					out[i] = float64(v.Integer)
				} else {
					out[i] = v.Double
				}
			}
			return DoubleArrayValue(out)
		}
		out := make([]string, len(vals))
		for i, v := range vals {
			out[i] = v.AsString()
		}
		return StringArrayValue(out)
	}
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return StringValue(s[1 : len(s)-1])
	}
	switch strings.ToLower(s) {
	case "true", "yes", "on":
		return BoolValue(true)
	case "false", "no", "off":
		return BoolValue(false)
	}
	if v, err := strconv.ParseInt(s, 0, 64); err == nil {
		return IntegerValue(v)
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return DoubleValue(v)
	}
	return StringValue(s)
}
