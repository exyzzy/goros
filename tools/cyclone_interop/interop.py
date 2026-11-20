"""Cyclone DDS side of the goros RTPS interop check.

Subscribes to rt/chatter (std_msgs/String from goros), publishes
rt/to_goros, and prints what discovery sees.
"""
import sys, time
from dataclasses import dataclass

from cyclonedds.domain import DomainParticipant
from cyclonedds.topic import Topic
from cyclonedds.sub import DataReader
from cyclonedds.pub import DataWriter
from cyclonedds.idl import IdlStruct
from cyclonedds.idl.annotations import final
from cyclonedds.core import Qos, Policy
from cyclonedds.util import duration
from cyclonedds.builtin import (BuiltinDataReader, BuiltinTopicDcpsParticipant,
                                BuiltinTopicDcpsPublication, BuiltinTopicDcpsSubscription)


@final
@dataclass
class String_(IdlStruct, typename="std_msgs::msg::dds_::String_"):
    data: str


@final
@dataclass
class Int32_(IdlStruct, typename="std_msgs::msg::dds_::Int32_"):
    data: int  # int32 by default? use explicit types below


from cyclonedds.idl.types import int32, int64


@final
@dataclass
class Int32T(IdlStruct, typename="std_msgs::msg::dds_::Int32_"):
    data: int32


@final
@dataclass
class AddTwoIntsReq(IdlStruct, typename="example_interfaces::srv::dds_::AddTwoInts_Request_"):
    a: int64
    b: int64


@final
@dataclass
class AddTwoIntsResp(IdlStruct, typename="example_interfaces::srv::dds_::AddTwoInts_Response_"):
    sum: int64


DOMAIN = int(sys.argv[1]) if len(sys.argv) > 1 else 93
RUN = float(sys.argv[2]) if len(sys.argv) > 2 else 20

dp = DomainParticipant(DOMAIN)
reliable = Qos(Policy.Reliability.Reliable(duration(seconds=1)), Policy.History.KeepLast(10))
latched = Qos(Policy.Reliability.Reliable(duration(seconds=1)), Policy.Durability.TransientLocal, Policy.History.KeepLast(10),
              Policy.DurabilityService(0, Policy.History.KeepLast(10), 10, 10, 10))

chatter = DataReader(dp, Topic(dp, "rt/chatter", String_), qos=reliable)
latched_rd = DataReader(dp, Topic(dp, "rt/latched", Int32T), qos=latched)
to_goros = DataWriter(dp, Topic(dp, "rt/to_goros", String_), qos=reliable)
req_wr = DataWriter(dp, Topic(dp, "rq/add_two_intsRequest", AddTwoIntsReq), qos=reliable)
resp_rd = DataReader(dp, Topic(dp, "rr/add_two_intsReply", AddTwoIntsResp), qos=reliable)
replies = []
latched_wr = DataWriter(dp, Topic(dp, "rt/latched_from_dds", Int32T), qos=latched)
for i in (10, 20, 30):
    latched_wr.write(Int32T(data=i))

parts = BuiltinDataReader(dp, BuiltinTopicDcpsParticipant)
pubs = BuiltinDataReader(dp, BuiltinTopicDcpsPublication)
subs = BuiltinDataReader(dp, BuiltinTopicDcpsSubscription)

got = []
latched_got = []
seen_pubs, seen_subs, seen_parts = set(), set(), set()
t0 = time.time()
n = 0
while time.time() - t0 < RUN:
    for s in chatter.take(N=100):
        got.append(s.data)
        print("cyclone got:", s.data, flush=True)
    for s in latched_rd.take(N=100):
        latched_got.append(s.data)
        print("cyclone latched:", s.data, flush=True)
    for p in parts.take(N=100):
        seen_parts.add(str(p.key))
    for p in pubs.take(N=100):
        if p.topic_name not in seen_pubs:
            print("cyclone sees publication:", p.topic_name, p.type_name, flush=True)
        seen_pubs.add(p.topic_name)
    for p in subs.take(N=100):
        if p.topic_name not in seen_subs:
            print("cyclone sees subscription:", p.topic_name, p.type_name, flush=True)
        seen_subs.add(p.topic_name)
    if n % 5 == 0:
        to_goros.write(String_(data=f"from cyclone {n // 5}"))
    if n == 20 or n == 30:
        req_wr.write(AddTwoIntsReq(a=n, b=1))
        print("cyclone sent request", n, "+ 1", flush=True)
    for r in resp_rd.take(N=10):
        replies.append(r.sum)
        print("cyclone got reply sum =", r.sum, flush=True)
    n += 1
    time.sleep(0.2)

print("SUMMARY chatter=%d latched=%s replies=%s pubs=%d subs=%d" % (len(got), latched_got, replies, len(seen_pubs), len(seen_subs)), flush=True)
