# Cyclone DDS interoperability check

Exercises goros' RTPS transport against a real DDS implementation without
a ROS 2 installation: the Cyclone DDS Python binding ships its own native
library.

```sh
python3 -m venv venv && venv/bin/pip install cyclonedds
go build -o rtps_interop ../../examples/rtps_interop

venv/bin/python -I interop.py 93 20 &     # domain 93, run 20 s
ROS_DOMAIN_ID=93 ./rtps_interop 15s
```

Expected, in either start-up order: Cyclone lists every goros endpoint
with ROS 2 topic and type names, prints `cyclone got: hello from goros N`,
`cyclone latched: 1 2 3` and `cyclone got reply sum = 21`/`31`; goros
prints `goros got: from cyclone N`, `goros got latched: 10 20 30` and
`goros served add_two_ints`. The final `SUMMARY` line from the Python side
gives the counts.

Set `GOROS_RTPS_DEBUG=1` for the goros side's protocol trace, and
`CYCLONEDDS_URI='<Tracing><Verbosity>finest</Verbosity><OutputFile>cyclone.trace</OutputFile></Tracing>'`
for Cyclone's.
