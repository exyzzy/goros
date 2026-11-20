package goros

import (
	"sort"

	"bitbucket.org/goros/names"
	"bitbucket.org/goros/transport"
)

// TopicEndpointInfo describes one publisher or subscription in the graph.
type TopicEndpointInfo struct {
	NodeName      string
	NodeNamespace string
	TopicType     string
	QoS           QoSProfile
	Participant   transport.GUID
}

// NodeNames lists fully-qualified names of all nodes in the graph.
func (c *Context) NodeNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	set := map[string]bool{}
	for _, n := range c.nodes {
		set[n.FullyQualifiedName()] = true
	}
	for _, r := range c.remotes {
		for _, n := range r.info.Nodes {
			set[names.FullyQualifiedNodeName(n.Name, n.Namespace)] = true
		}
	}
	return sortedKeys(set)
}

// TopicNamesAndTypes lists every topic with its message type(s).
func (c *Context) TopicNamesAndTypes() map[string][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := map[string]map[string]bool{}
	add := func(name, typ string) {
		if out[name] == nil {
			out[name] = map[string]bool{}
		}
		out[name][typ] = true
	}
	for t, list := range c.pubs {
		for _, p := range list {
			add(t, p.typ)
		}
	}
	for t, list := range c.subs {
		for _, s := range list {
			add(t, s.typ)
		}
	}
	for _, r := range c.remotes {
		for _, n := range r.info.Nodes {
			for _, e := range n.Publishers {
				add(e.Name, e.Type)
			}
			for _, e := range n.Subscriptions {
				add(e.Name, e.Type)
			}
		}
	}
	return flatten(out)
}

// ServiceNamesAndTypes lists every service with its type(s).
func (c *Context) ServiceNamesAndTypes() map[string][]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := map[string]map[string]bool{}
	for name, s := range c.services {
		out[name] = map[string]bool{s.typ: true}
	}
	for _, r := range c.remotes {
		for _, n := range r.info.Nodes {
			for _, e := range n.Services {
				if out[e.Name] == nil {
					out[e.Name] = map[string]bool{}
				}
				out[e.Name][e.Type] = true
			}
		}
	}
	return flatten(out)
}

// PublishersInfo lists publishers on a topic.
func (c *Context) PublishersInfo(topic string) []TopicEndpointInfo {
	return c.endpointsInfo(topic, true)
}

// SubscriptionsInfo lists subscriptions on a topic.
func (c *Context) SubscriptionsInfo(topic string) []TopicEndpointInfo {
	return c.endpointsInfo(topic, false)
}

func (c *Context) endpointsInfo(topic string, pubs bool) []TopicEndpointInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []TopicEndpointInfo
	for _, n := range c.nodes {
		ni := n.info()
		list := ni.Subscriptions
		if pubs {
			list = ni.Publishers
		}
		for _, e := range list {
			if e.Name == topic {
				out = append(out, TopicEndpointInfo{n.name, n.namespace, e.Type, qosFromInfo(e.QoS), c.guid})
			}
		}
	}
	for g, r := range c.remotes {
		for _, n := range r.info.Nodes {
			list := n.Subscriptions
			if pubs {
				list = n.Publishers
			}
			for _, e := range list {
				if e.Name == topic {
					out = append(out, TopicEndpointInfo{n.Name, n.Namespace, e.Type, qosFromInfo(e.QoS), g})
				}
			}
		}
	}
	return out
}

func (c *Context) countPublishers(topic string) int  { return len(c.PublishersInfo(topic)) }
func (c *Context) countSubscribers(topic string) int { return len(c.SubscriptionsInfo(topic)) }

// CountPublishers counts publishers on a topic (graph-wide).
func (c *Context) CountPublishers(topic string) int { return c.countPublishers(topic) }

// CountSubscribers counts subscriptions on a topic (graph-wide).
func (c *Context) CountSubscribers(topic string) int { return c.countSubscribers(topic) }

// GraphChanged returns a channel closed on the next discovery change, for
// waiting on nodes/topics to appear.
func (c *Context) GraphChanged() <-chan struct{} { return c.graphChanged() }

// Node-level convenience wrappers mirroring rclcpp::Node's graph API.
func (n *Node) GetNodeNames() []string                     { return n.ctx.NodeNames() }
func (n *Node) GetTopicNamesAndTypes() map[string][]string { return n.ctx.TopicNamesAndTypes() }
func (n *Node) GetServiceNamesAndTypes() map[string][]string {
	return n.ctx.ServiceNamesAndTypes()
}
func (n *Node) CountPublishers(topic string) int  { return n.ctx.CountPublishers(topic) }
func (n *Node) CountSubscribers(topic string) int { return n.ctx.CountSubscribers(topic) }

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func flatten(m map[string]map[string]bool) map[string][]string {
	out := make(map[string][]string, len(m))
	for k, v := range m {
		out[k] = sortedKeys(v)
	}
	return out
}
