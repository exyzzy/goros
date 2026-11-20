package goros

import "bitbucket.org/goros/transport"

// Reliability policy.
type Reliability uint8

const (
	ReliabilitySystemDefault Reliability = 0
	Reliable                 Reliability = 1
	BestEffort               Reliability = 2
)

// Durability policy.
type Durability uint8

const (
	DurabilitySystemDefault Durability = 0
	TransientLocal          Durability = 1
	Volatile                Durability = 2
)

// History policy.
type History uint8

const (
	HistorySystemDefault History = 0
	KeepLast             History = 1
	KeepAll              History = 2
)

// QoSProfile is the subset of DDS Quality of Service that goros honours.
//
//   - History/Depth bound the per-subscription receive queue and the
//     transient-local publisher cache.
//   - Reliability is advisory: intra-process, in-process and serial links are
//     always reliable, UDP multicast is best-effort. Endpoints with
//     incompatible policies (a Reliable subscription and a BestEffort
//     publisher, or a TransientLocal subscription and a Volatile publisher)
//     do not match, as in ROS 2.
//   - Durability TransientLocal makes publishers replay their last Depth
//     samples to late-joining subscriptions.
type QoSProfile struct {
	History     History
	Depth       int
	Reliability Reliability
	Durability  Durability
}

// Standard profiles matching rmw_qos_profile_*.
var (
	QoSDefault         = QoSProfile{KeepLast, 10, Reliable, Volatile}
	QoSSensorData      = QoSProfile{KeepLast, 5, BestEffort, Volatile}
	QoSServicesDefault = QoSProfile{KeepLast, 10, Reliable, Volatile}
	QoSParameters      = QoSProfile{KeepLast, 1000, Reliable, Volatile}
	QoSParameterEvents = QoSProfile{KeepLast, 1000, Reliable, Volatile}
	QoSRosout          = QoSProfile{KeepLast, 1000, Reliable, Volatile}
	QoSSystemDefault   = QoSProfile{HistorySystemDefault, 0, ReliabilitySystemDefault, DurabilitySystemDefault}
)

// KeepLastQoS returns the default profile with the given depth.
func KeepLastQoS(depth int) QoSProfile {
	q := QoSDefault
	q.Depth = depth
	return q
}

// Resolve replaces SystemDefault settings with the concrete defaults.
func (q QoSProfile) Resolve() QoSProfile {
	if q.History == HistorySystemDefault {
		q.History = KeepLast
	}
	if q.Depth <= 0 {
		q.Depth = 10
	}
	if q.Reliability == ReliabilitySystemDefault {
		q.Reliability = Reliable
	}
	if q.Durability == DurabilitySystemDefault {
		q.Durability = Volatile
	}
	return q
}

// CompatibleWith reports whether a publisher with profile q can deliver to a
// subscription with profile sub (DDS request/offer semantics).
func (q QoSProfile) CompatibleWith(sub QoSProfile) bool {
	q, sub = q.Resolve(), sub.Resolve()
	if sub.Reliability == Reliable && q.Reliability == BestEffort {
		return false
	}
	if sub.Durability == TransientLocal && q.Durability == Volatile {
		return false
	}
	return true
}

func (q QoSProfile) info() transport.QoSInfo {
	q = q.Resolve()
	return transport.QoSInfo{Reliability: uint8(q.Reliability), Durability: uint8(q.Durability), History: uint8(q.History), Depth: uint32(q.Depth)}
}

func qosFromInfo(i transport.QoSInfo) QoSProfile {
	return QoSProfile{History: History(i.History), Depth: int(i.Depth), Reliability: Reliability(i.Reliability), Durability: Durability(i.Durability)}
}
