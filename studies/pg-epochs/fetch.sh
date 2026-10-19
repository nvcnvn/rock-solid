#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

ceph=b7e6787889ed0783df44063506dcacaaf672758f
vitastor=df593badb5dfdbfbacac97b0c310f6ea3d5c3dff

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

for p in doc/dev/peering.rst doc/dev/osd_internals/map_message_handling.rst \
	src/include/types.h src/osd/osd_types.h src/osd/osd_types.cc src/osd/PG.h src/osd/PG.cc \
	src/osd/PeeringState.h src/osd/PeeringState.cc src/osd/PrimaryLogPG.cc src/osd/ReplicatedBackend.cc src/osd/OSD.cc \
	src/osdc/Objecter.cc src/common/options/global.yaml.in src/common/options/osd.yaml.in \
	src/common/options/mon.yaml.in; do
	get ceph/ceph $ceph "$p"
done
for p in src/osd/osd_peering_pg.h src/osd/osd_peering_pg.cpp src/osd/osd_primary_write.cpp \
	src/osd/osd_cluster.cpp; do
	get vitalif/vitastor $vitastor "$p"
done

show() { # file first last
	echo "== $1:$2-$3"
	sed -n "$2,$3p" "$1"
}

c=ceph_ceph_
show ${c}doc_dev_peering.rst 37 40
show ${c}doc_dev_peering.rst 91 104
show ${c}doc_dev_peering.rst 139 160
show ${c}doc_dev_osd_internals_map_message_handling.rst 54 58
show ${c}src_include_types.h 82 83
show ${c}src_osd_osd_types.h 838 841
show ${c}src_osd_osd_types.h 890 892
show ${c}src_osd_osd_types.h 2827 2844
show ${c}src_osd_osd_types.cc 1413 1415
show ${c}src_osd_osd_types.cc 4695 4721
show ${c}src_osd_osd_types.cc 4791 4825
show ${c}src_osd_osd_types.cc 4867 4885
show ${c}src_osd_PG.h 1338 1345
show ${c}src_osd_PG.cc 1868 1877
show ${c}src_osd_PG.cc 1965 1977
show ${c}src_osd_PG.cc 2029 2066
show ${c}src_osd_PeeringState.cc 628 632
show ${c}src_osd_PeeringState.cc 790 814
show ${c}src_osd_PeeringState.cc 1499 1510
show ${c}src_osd_PrimaryLogPG.cc 13165 13185
show ${c}src_osd_PrimaryLogPG.cc 13475 13475
show ${c}src_osd_ReplicatedBackend.cc 1168 1174
show ${c}src_osd_OSD.cc 7684 7686
show ${c}src_osd_OSD.cc 8412 8420
show ${c}src_osd_OSD.cc 8454 8463
show ${c}src_osd_OSD.cc 9057 9064
show ${c}src_osdc_Objecter.cc 3246 3279
show ${c}src_common_options_global.yaml.in 1804 1810
show ${c}src_common_options_global.yaml.in 2961 2966
show ${c}src_common_options_osd.yaml.in 927 937
show ${c}src_common_options_mon.yaml.in 1079 1086
v=vitalif_vitastor_
show ${v}src_osd_osd_peering_pg.h 15 15
show ${v}src_osd_osd_peering_pg.h 94 95
show ${v}src_osd_osd_peering_pg.cpp 426 449
show ${v}src_osd_osd_primary_write.cpp 201 240
show ${v}src_osd_osd_cluster.cpp 1135 1146
