package goros

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"

	"bitbucket.org/goros/msgs/rcl_interfaces"
)

// ParameterType enumerates rcl_interfaces/msg/ParameterType.
type ParameterType uint8

const (
	ParameterNotSet       ParameterType = 0
	ParameterBool         ParameterType = 1
	ParameterInteger      ParameterType = 2
	ParameterDouble       ParameterType = 3
	ParameterString       ParameterType = 4
	ParameterByteArray    ParameterType = 5
	ParameterBoolArray    ParameterType = 6
	ParameterIntegerArray ParameterType = 7
	ParameterDoubleArray  ParameterType = 8
	ParameterStringArray  ParameterType = 9
)

func (t ParameterType) String() string {
	switch t {
	case ParameterBool:
		return "bool"
	case ParameterInteger:
		return "integer"
	case ParameterDouble:
		return "double"
	case ParameterString:
		return "string"
	case ParameterByteArray:
		return "byte_array"
	case ParameterBoolArray:
		return "bool_array"
	case ParameterIntegerArray:
		return "integer_array"
	case ParameterDoubleArray:
		return "double_array"
	case ParameterStringArray:
		return "string_array"
	}
	return "not_set"
}

// ParameterValue is a tagged union of the ROS parameter types.
type ParameterValue struct {
	Type         ParameterType
	Bool         bool
	Integer      int64
	Double       float64
	String       string
	ByteArray    []byte
	BoolArray    []bool
	IntegerArray []int64
	DoubleArray  []float64
	StringArray  []string
}

func BoolValue(v bool) ParameterValue      { return ParameterValue{Type: ParameterBool, Bool: v} }
func IntegerValue(v int64) ParameterValue  { return ParameterValue{Type: ParameterInteger, Integer: v} }
func DoubleValue(v float64) ParameterValue { return ParameterValue{Type: ParameterDouble, Double: v} }
func StringValue(v string) ParameterValue  { return ParameterValue{Type: ParameterString, String: v} }
func ByteArrayValue(v []byte) ParameterValue {
	return ParameterValue{Type: ParameterByteArray, ByteArray: v}
}
func BoolArrayValue(v []bool) ParameterValue {
	return ParameterValue{Type: ParameterBoolArray, BoolArray: v}
}
func IntegerArrayValue(v []int64) ParameterValue {
	return ParameterValue{Type: ParameterIntegerArray, IntegerArray: v}
}
func DoubleArrayValue(v []float64) ParameterValue {
	return ParameterValue{Type: ParameterDoubleArray, DoubleArray: v}
}
func StringArrayValue(v []string) ParameterValue {
	return ParameterValue{Type: ParameterStringArray, StringArray: v}
}

// AsString renders the value for display.
func (v ParameterValue) AsString() string {
	switch v.Type {
	case ParameterBool:
		return strconv.FormatBool(v.Bool)
	case ParameterInteger:
		return strconv.FormatInt(v.Integer, 10)
	case ParameterDouble:
		return strconv.FormatFloat(v.Double, 'g', -1, 64)
	case ParameterString:
		return v.String
	case ParameterByteArray:
		parts := make([]string, len(v.ByteArray))
		for i, b := range v.ByteArray {
			parts[i] = strconv.Itoa(int(b))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case ParameterBoolArray:
		parts := make([]string, len(v.BoolArray))
		for i, b := range v.BoolArray {
			parts[i] = strconv.FormatBool(b)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case ParameterIntegerArray:
		parts := make([]string, len(v.IntegerArray))
		for i, b := range v.IntegerArray {
			parts[i] = strconv.FormatInt(b, 10)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case ParameterDoubleArray:
		parts := make([]string, len(v.DoubleArray))
		for i, b := range v.DoubleArray {
			parts[i] = strconv.FormatFloat(b, 'g', -1, 64)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case ParameterStringArray:
		return "[" + strings.Join(v.StringArray, ", ") + "]"
	}
	return "<not set>"
}

// ToMsg converts to the rcl_interfaces wire representation.
func (v ParameterValue) ToMsg() rcl_interfaces.ParameterValue {
	return rcl_interfaces.ParameterValue{
		Type: uint8(v.Type), BoolValue: v.Bool, IntegerValue: v.Integer, DoubleValue: v.Double, StringValue: v.String,
		ByteArrayValue: v.ByteArray, BoolArrayValue: v.BoolArray, IntegerArrayValue: v.IntegerArray,
		DoubleArrayValue: v.DoubleArray, StringArrayValue: v.StringArray,
	}
}

// ParameterValueFromMsg converts from the rcl_interfaces representation.
func ParameterValueFromMsg(m rcl_interfaces.ParameterValue) ParameterValue {
	return ParameterValue{
		Type: ParameterType(m.Type), Bool: m.BoolValue, Integer: m.IntegerValue, Double: m.DoubleValue, String: m.StringValue,
		ByteArray: m.ByteArrayValue, BoolArray: m.BoolArrayValue, IntegerArray: m.IntegerArrayValue,
		DoubleArray: m.DoubleArrayValue, StringArray: m.StringArrayValue,
	}
}

// Parameter is a named value.
type Parameter struct {
	Name  string
	Value ParameterValue
}

// ParameterDescriptor carries metadata about a declared parameter.
type ParameterDescriptor struct {
	Description           string
	AdditionalConstraints string
	ReadOnly              bool
	DynamicTyping         bool
}

// SetParametersResult is returned by set-parameter callbacks.
type SetParametersResult struct {
	Successful bool
	Reason     string
}

// OnSetParametersCallback validates a batch of parameter changes before they
// are applied. Returning an unsuccessful result rejects the whole batch.
type OnSetParametersCallback func(params []Parameter) SetParametersResult

var (
	ErrParameterNotDeclared     = errors.New("goros: parameter not declared")
	ErrParameterAlreadyDeclared = errors.New("goros: parameter already declared")
	ErrParameterReadOnly        = errors.New("goros: parameter is read-only")
	ErrParameterType            = errors.New("goros: parameter type mismatch")
	ErrParameterRejected        = errors.New("goros: parameter change rejected")
)

type paramEntry struct {
	value ParameterValue
	desc  ParameterDescriptor
}

type paramStore struct {
	mu        sync.Mutex
	params    map[string]*paramEntry
	overrides map[string]ParameterValue
	callbacks []OnSetParametersCallback
}

func newParamStore(overrides map[string]ParameterValue) *paramStore {
	if overrides == nil {
		overrides = map[string]ParameterValue{}
	}
	return &paramStore{params: map[string]*paramEntry{}, overrides: overrides}
}

// DeclareParameter registers a parameter with a default value. An override
// given on the command line (-p name:=value) or via WithParameterOverrides
// takes precedence over the default. The effective value is returned.
func (n *Node) DeclareParameter(name string, def ParameterValue, desc ...ParameterDescriptor) (ParameterValue, error) {
	ps := n.params
	ps.mu.Lock()
	if _, ok := ps.params[name]; ok {
		ps.mu.Unlock()
		return ParameterValue{}, ErrParameterAlreadyDeclared
	}
	e := &paramEntry{value: def}
	if len(desc) > 0 {
		e.desc = desc[0]
	}
	if ov, ok := ps.overrides[name]; ok {
		if def.Type == ParameterNotSet || ov.Type == def.Type || e.desc.DynamicTyping {
			e.value = ov
		} else if conv, ok := coerce(ov, def.Type); ok {
			e.value = conv
		}
	}
	ps.params[name] = e
	ps.mu.Unlock()
	n.publishParameterEvent([]Parameter{{name, e.value}}, nil, nil)
	return e.value, nil
}

// coerce handles the common case of an integer override for a double
// parameter.
func coerce(v ParameterValue, want ParameterType) (ParameterValue, bool) {
	if want == ParameterDouble && v.Type == ParameterInteger {
		return DoubleValue(float64(v.Integer)), true
	}
	if want == ParameterDoubleArray && v.Type == ParameterIntegerArray {
		out := make([]float64, len(v.IntegerArray))
		for i, x := range v.IntegerArray {
			out[i] = float64(x)
		}
		return DoubleArrayValue(out), true
	}
	return v, false
}

// HasParameter reports whether name is declared.
func (n *Node) HasParameter(name string) bool {
	n.params.mu.Lock()
	defer n.params.mu.Unlock()
	_, ok := n.params.params[name]
	return ok
}

// GetParameter returns a declared parameter's value.
func (n *Node) GetParameter(name string) (ParameterValue, error) {
	n.params.mu.Lock()
	defer n.params.mu.Unlock()
	e, ok := n.params.params[name]
	if !ok {
		return ParameterValue{}, ErrParameterNotDeclared
	}
	return e.value, nil
}

// GetParameterOr returns the parameter value or def if not declared.
func (n *Node) GetParameterOr(name string, def ParameterValue) ParameterValue {
	if v, err := n.GetParameter(name); err == nil {
		return v
	}
	return def
}

// SetParameter sets one parameter, running validation callbacks.
func (n *Node) SetParameter(p Parameter) error {
	res := n.SetParameters([]Parameter{p})
	if !res[0].Successful {
		return errors.New(res[0].Reason)
	}
	return nil
}

// SetParameters sets each parameter individually and returns one result per
// parameter, like rclcpp's set_parameters.
func (n *Node) SetParameters(params []Parameter) []SetParametersResult {
	out := make([]SetParametersResult, len(params))
	for i, p := range params {
		out[i] = n.SetParametersAtomically([]Parameter{p})
	}
	return out
}

// SetParametersAtomically applies all parameters or none.
func (n *Node) SetParametersAtomically(params []Parameter) SetParametersResult {
	ps := n.params
	ps.mu.Lock()
	for _, p := range params {
		e, ok := ps.params[p.Name]
		if !ok {
			ps.mu.Unlock()
			return SetParametersResult{false, "parameter '" + p.Name + "' is not declared"}
		}
		if e.desc.ReadOnly {
			ps.mu.Unlock()
			return SetParametersResult{false, "parameter '" + p.Name + "' is read-only"}
		}
		if e.value.Type != ParameterNotSet && p.Value.Type != e.value.Type && !e.desc.DynamicTyping {
			if _, ok := coerce(p.Value, e.value.Type); !ok {
				ps.mu.Unlock()
				return SetParametersResult{false, "parameter '" + p.Name + "' has type " + e.value.Type.String() + ", got " + p.Value.Type.String()}
			}
		}
	}
	cbs := append([]OnSetParametersCallback(nil), ps.callbacks...)
	ps.mu.Unlock()
	for _, cb := range cbs {
		if r := cb(params); !r.Successful {
			return r
		}
	}
	ps.mu.Lock()
	changed := make([]Parameter, 0, len(params))
	for _, p := range params {
		e := ps.params[p.Name]
		v := p.Value
		if e.value.Type != ParameterNotSet && v.Type != e.value.Type && !e.desc.DynamicTyping {
			v, _ = coerce(v, e.value.Type)
		}
		e.value = v
		changed = append(changed, Parameter{p.Name, v})
	}
	ps.mu.Unlock()
	n.publishParameterEvent(nil, changed, nil)
	return SetParametersResult{Successful: true}
}

// UndeclareParameter removes a parameter.
func (n *Node) UndeclareParameter(name string) error {
	ps := n.params
	ps.mu.Lock()
	e, ok := ps.params[name]
	if !ok {
		ps.mu.Unlock()
		return ErrParameterNotDeclared
	}
	if e.desc.ReadOnly {
		ps.mu.Unlock()
		return ErrParameterReadOnly
	}
	delete(ps.params, name)
	ps.mu.Unlock()
	n.publishParameterEvent(nil, nil, []Parameter{{name, e.value}})
	return nil
}

// DescribeParameter returns a parameter's descriptor.
func (n *Node) DescribeParameter(name string) (ParameterDescriptor, error) {
	n.params.mu.Lock()
	defer n.params.mu.Unlock()
	e, ok := n.params.params[name]
	if !ok {
		return ParameterDescriptor{}, ErrParameterNotDeclared
	}
	return e.desc, nil
}

// ListParameters returns declared parameter names matching any prefix
// (all when prefixes is empty), sorted. depth 0 means unlimited.
func (n *Node) ListParameters(prefixes []string, depth int) []string {
	n.params.mu.Lock()
	defer n.params.mu.Unlock()
	var out []string
	for name := range n.params.params {
		if len(prefixes) > 0 {
			ok := false
			for _, p := range prefixes {
				if strings.HasPrefix(name, p+".") || name == p {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		if depth > 0 && strings.Count(name, ".") >= depth {
			continue
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// AddOnSetParametersCallback registers a validator invoked before any
// parameter change is applied.
func (n *Node) AddOnSetParametersCallback(cb OnSetParametersCallback) {
	n.params.mu.Lock()
	n.params.callbacks = append(n.params.callbacks, cb)
	n.params.mu.Unlock()
}

func toParamMsgs(ps []Parameter) []rcl_interfaces.Parameter {
	out := make([]rcl_interfaces.Parameter, len(ps))
	for i, p := range ps {
		out[i] = rcl_interfaces.Parameter{Name: p.Name, Value: p.Value.ToMsg()}
	}
	return out
}

func (n *Node) publishParameterEvent(added, changed, deleted []Parameter) {
	pub := n.paramEvents
	if pub == nil {
		return
	}
	_ = pub.Publish(&rcl_interfaces.ParameterEvent{
		Stamp: n.Now(), Node: n.FullyQualifiedName(),
		NewParameters: toParamMsgs(added), ChangedParameters: toParamMsgs(changed), DeletedParameters: toParamMsgs(deleted),
	})
}
