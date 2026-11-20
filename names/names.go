// Package names implements ROS 2 topic, service and node name validation,
// namespace expansion and ~/private name handling, following the rules in
// the "Topic and Service name mapping" design document.
package names

import (
	"errors"
	"strings"
)

var (
	ErrEmpty          = errors.New("names: name is empty")
	ErrBadChar        = errors.New("names: name contains invalid characters")
	ErrLeadingDigit   = errors.New("names: token starts with a digit")
	ErrDoubleSlash    = errors.New("names: name contains repeated '/'")
	ErrTrailingSlash  = errors.New("names: name ends with '/'")
	ErrTilde          = errors.New("names: '~' is only allowed at the start of a name")
	ErrNotAbsolute    = errors.New("names: namespace must start with '/'")
	ErrUnbalancedSubs = errors.New("names: unbalanced '{' '}' substitution")
)

func isAlpha(c byte) bool { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// ValidateTopic checks a (possibly relative or private) topic or service
// name before expansion.
func ValidateTopic(name string) error {
	if name == "" {
		return ErrEmpty
	}
	if strings.Contains(name, "//") {
		return ErrDoubleSlash
	}
	if len(name) > 1 && strings.HasSuffix(name, "/") {
		return ErrTrailingSlash
	}
	depth := 0
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '~':
			if i != 0 {
				return ErrTilde
			}
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth < 0 {
				return ErrUnbalancedSubs
			}
		case c == '/' || c == '_' || isAlpha(c) || isDigit(c):
		default:
			return ErrBadChar
		}
	}
	if depth != 0 {
		return ErrUnbalancedSubs
	}
	// Each token must not start with a digit.
	for _, tok := range strings.Split(strings.TrimPrefix(name, "~"), "/") {
		if tok != "" && isDigit(tok[0]) {
			return ErrLeadingDigit
		}
	}
	return nil
}

// ValidateFullTopic checks a fully expanded (absolute) topic name.
func ValidateFullTopic(name string) error {
	if err := ValidateTopic(name); err != nil {
		return err
	}
	if !strings.HasPrefix(name, "/") {
		return ErrNotAbsolute
	}
	if strings.ContainsAny(name, "~{}") {
		return ErrBadChar
	}
	return nil
}

// ValidateNodeName checks a node name: alphanumerics and underscores only.
func ValidateNodeName(name string) error {
	if name == "" {
		return ErrEmpty
	}
	if isDigit(name[0]) {
		return ErrLeadingDigit
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !(c == '_' || isAlpha(c) || isDigit(c)) {
			return ErrBadChar
		}
	}
	return nil
}

// ValidateNamespace checks a namespace: absolute, no trailing slash
// (except the root "/"), valid tokens.
func ValidateNamespace(ns string) error {
	if ns == "" {
		return ErrEmpty
	}
	if !strings.HasPrefix(ns, "/") {
		return ErrNotAbsolute
	}
	if ns == "/" {
		return nil
	}
	if strings.HasSuffix(ns, "/") {
		return ErrTrailingSlash
	}
	if strings.Contains(ns, "//") {
		return ErrDoubleSlash
	}
	for _, tok := range strings.Split(ns[1:], "/") {
		if err := ValidateNodeName(tok); err != nil {
			return err
		}
	}
	return nil
}

// Expand converts name into a fully qualified name using the node's name
// and namespace:
//
//	"/abs"   -> "/abs"
//	"rel"    -> "<ns>/rel"
//	"~/priv" -> "<ns>/<node>/priv"
//	"~"      -> "<ns>/<node>"
//
// The substitutions {node}, {ns} and {namespace} are replaced.
func Expand(name, node, ns string) (string, error) {
	if err := ValidateTopic(name); err != nil {
		return "", err
	}
	if ns == "" {
		ns = "/"
	}
	name = strings.ReplaceAll(name, "{node}", node)
	name = strings.ReplaceAll(name, "{namespace}", ns)
	name = strings.ReplaceAll(name, "{ns}", ns)

	var full string
	switch {
	case strings.HasPrefix(name, "/"):
		full = name
	case name == "~":
		full = Join(ns, node)
	case strings.HasPrefix(name, "~/"):
		full = Join(Join(ns, node), name[2:])
	default:
		full = Join(ns, name)
	}
	// {ns} substitution of "/" can create "//".
	for strings.Contains(full, "//") {
		full = strings.ReplaceAll(full, "//", "/")
	}
	if err := ValidateFullTopic(full); err != nil {
		return "", err
	}
	return full, nil
}

// Join concatenates a namespace and a relative name with exactly one '/'.
func Join(ns, rel string) string {
	if ns == "" || ns == "/" {
		return "/" + strings.TrimPrefix(rel, "/")
	}
	return strings.TrimSuffix(ns, "/") + "/" + strings.TrimPrefix(rel, "/")
}

// FullyQualifiedNodeName returns "<ns>/<node>".
func FullyQualifiedNodeName(node, ns string) string { return Join(ns, node) }

// IsHidden reports whether any token of the name starts with '_', which
// ROS 2 tooling hides by default (e.g. /_internal, /rosout is not hidden).
func IsHidden(name string) bool {
	for _, tok := range strings.Split(name, "/") {
		if strings.HasPrefix(tok, "_") {
			return true
		}
	}
	return false
}
