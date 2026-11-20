package rtps

import "strings"

// ROS 2 maps its names onto DDS as rmw_fastrtps and rmw_cyclonedds do:
//
//	topic   /chatter                      -> rt/chatter
//	service /add_two_ints request topic   -> rq/add_two_intsRequest
//	service /add_two_ints reply topic     -> rr/add_two_intsReply
//	type    std_msgs/msg/String           -> std_msgs::msg::dds_::String_
//	type    example_interfaces/srv/AddTwoInts request -> example_interfaces::srv::dds_::AddTwoInts_Request_
const (
	topicPrefix   = "rt"
	requestPrefix = "rq"
	replyPrefix   = "rr"
	requestSuffix = "Request"
	replySuffix   = "Reply"

	discoveryInfoTopic = "ros_discovery_info"
	discoveryInfoType  = "rmw_dds_common::msg::dds_::ParticipantEntitiesInfo_"
)

func ddsTopic(ros string) string { return topicPrefix + ros }

func ddsRequestTopic(service string) string { return requestPrefix + service + requestSuffix }
func ddsReplyTopic(service string) string   { return replyPrefix + service + replySuffix }

// ddsType converts "pkg/msg/Name" to "pkg::msg::dds_::Name_".
func ddsType(ros string) string {
	i := strings.LastIndexByte(ros, '/')
	if i < 0 {
		return ros
	}
	return strings.ReplaceAll(ros[:i], "/", "::") + "::dds_::" + ros[i+1:] + "_"
}

func ddsRequestType(service string) string  { return ddsType(service + "_Request") }
func ddsResponseType(service string) string { return ddsType(service + "_Response") }

// rosTopic converts "rt/x" to "/x"; ok is false for non-ROS topics.
func rosTopic(dds string) (string, bool) {
	if strings.HasPrefix(dds, topicPrefix+"/") {
		return dds[len(topicPrefix):], true
	}
	return "", false
}

// rosService recognises rq/<name>Request and rr/<name>Reply topics and
// returns the service name. request reports which half it was.
func rosService(dds string) (name string, request, ok bool) {
	switch {
	case strings.HasPrefix(dds, requestPrefix+"/") && strings.HasSuffix(dds, requestSuffix):
		return dds[len(requestPrefix) : len(dds)-len(requestSuffix)], true, true
	case strings.HasPrefix(dds, replyPrefix+"/") && strings.HasSuffix(dds, replySuffix):
		return dds[len(replyPrefix) : len(dds)-len(replySuffix)], false, true
	}
	return "", false, false
}

// rosType converts "pkg::msg::dds_::Name_" to "pkg/msg/Name". Unknown
// shapes are returned unchanged.
func rosType(dds string) string {
	i := strings.Index(dds, "::dds_::")
	if i < 0 || !strings.HasSuffix(dds, "_") {
		return dds
	}
	return strings.ReplaceAll(dds[:i], "::", "/") + "/" + dds[i+len("::dds_::"):len(dds)-1]
}

// rosServiceType strips the _Request/_Response suffix from a demangled type.
func rosServiceType(rosT string) string {
	for _, s := range []string{"_Request", "_Response"} {
		if strings.HasSuffix(rosT, s) {
			return rosT[:len(rosT)-len(s)]
		}
	}
	return rosT
}
