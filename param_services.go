package goros

import (
	"bitbucket.org/goros/msgs/rcl_interfaces"
)

// createParameterServices exposes the node's parameters over the standard
// ~/get_parameters, ~/set_parameters, ~/set_parameters_atomically,
// ~/list_parameters, ~/describe_parameters and ~/get_parameter_types
// services, so `goros param` and ros2-style tooling can reach them.
func (n *Node) createParameterServices() {
	NewService(n, "~/get_parameters", func(req *rcl_interfaces.GetParameters_Request, resp *rcl_interfaces.GetParameters_Response) error {
		resp.Values = make([]rcl_interfaces.ParameterValue, len(req.Names))
		for i, name := range req.Names {
			if v, err := n.GetParameter(name); err == nil {
				resp.Values[i] = v.ToMsg()
			}
		}
		return nil
	})
	NewService(n, "~/get_parameter_types", func(req *rcl_interfaces.GetParameterTypes_Request, resp *rcl_interfaces.GetParameterTypes_Response) error {
		resp.Types = make([]uint8, len(req.Names))
		for i, name := range req.Names {
			if v, err := n.GetParameter(name); err == nil {
				resp.Types[i] = uint8(v.Type)
			}
		}
		return nil
	})
	NewService(n, "~/set_parameters", func(req *rcl_interfaces.SetParameters_Request, resp *rcl_interfaces.SetParameters_Response) error {
		params := fromParamMsgs(req.Parameters)
		for _, r := range n.SetParameters(params) {
			resp.Results = append(resp.Results, rcl_interfaces.SetParametersResult{Successful: r.Successful, Reason: r.Reason})
		}
		return nil
	})
	NewService(n, "~/set_parameters_atomically", func(req *rcl_interfaces.SetParameters_Request, resp *rcl_interfaces.SetParameters_Response) error {
		r := n.SetParametersAtomically(fromParamMsgs(req.Parameters))
		resp.Results = []rcl_interfaces.SetParametersResult{{Successful: r.Successful, Reason: r.Reason}}
		return nil
	})
	NewService(n, "~/list_parameters", func(req *rcl_interfaces.ListParameters_Request, resp *rcl_interfaces.ListParameters_Response) error {
		resp.Result.Names = n.ListParameters(req.Prefixes, int(req.Depth))
		seen := map[string]bool{}
		for _, name := range resp.Result.Names {
			for i := 0; i < len(name); i++ {
				if name[i] == '.' && !seen[name[:i]] {
					seen[name[:i]] = true
					resp.Result.Prefixes = append(resp.Result.Prefixes, name[:i])
				}
			}
		}
		return nil
	})
	NewService(n, "~/describe_parameters", func(req *rcl_interfaces.DescribeParameters_Request, resp *rcl_interfaces.DescribeParameters_Response) error {
		for _, name := range req.Names {
			d := rcl_interfaces.ParameterDescriptor{Name: name}
			if desc, err := n.DescribeParameter(name); err == nil {
				v, _ := n.GetParameter(name)
				d.Type = uint8(v.Type)
				d.Description = desc.Description
				d.AdditionalConstraints = desc.AdditionalConstraints
				d.ReadOnly = desc.ReadOnly
				d.DynamicTyping = desc.DynamicTyping
			}
			resp.Descriptors = append(resp.Descriptors, d)
		}
		return nil
	})
}

func fromParamMsgs(ps []rcl_interfaces.Parameter) []Parameter {
	out := make([]Parameter, len(ps))
	for i, p := range ps {
		out[i] = Parameter{Name: p.Name, Value: ParameterValueFromMsg(p.Value)}
	}
	return out
}
