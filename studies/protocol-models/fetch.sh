#!/usr/bin/env bash
# Downloads every source the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl and python3 (for the two PDFs).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=${1:-"$here/work"}
mkdir -p "$out"
cd "$out"

crdb=d30c905fff79ef825adc96bcc647f1872a90f2ff
raft=f35a02212416d045c574e30aa59d4b07c4b8d732 # etcd-io/raft v3.6.0
ceph=b7e6787889ed0783df44063506dcacaaf672758f
pingcap=8ae629ddf9f5ad8efc763bff93e3e43ce0a3585d
mongo=74526c1201109405172eb845413154f547a815ee

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}
patch() { # repo full-sha
	local f="$(echo "$1" | tr / _)_$2.patch"
	[ -s "$f" ] || curl -sSfL -o "$f" "https://github.com/$1/commit/$2.patch"
}
url() { # file url
	[ -s "$1" ] || curl -sSfL -o "$1" "$2"
}

get cockroachdb/cockroach $crdb docs/tla-plus/README.md
get cockroachdb/cockroach $crdb docs/tla-plus/ParallelCommits/ParallelCommits.tla
get cockroachdb/cockroach $crdb docs/tla-plus/ParallelCommits/ParallelCommits.cfg
get cockroachdb/cockroach $crdb docs/tla-plus/ParallelCommits/ParallelCommits.toolbox/ParallelCommits___Model_1.launch
get cockroachdb/cockroach $crdb docs/tla-plus/StoreLiveness/StoreLiveness.cfg
get cockroachdb/cockroach $crdb pkg/kv/kvserver/storeliveness/doc.go
for c in af78fdcf89a076a4b4da0695660cace67ccd3aed 50e10374283e37525610eda141082871bf2a5554 \
	74c36e51a5b3825076c378834558de234b13feae 8dc06b6a49fbf43b6e81baa2693e5c890a36e21b \
	abc7f04791315c949bcd4fc13248ef41630eada3 ef953596d5a91a90a927182938a7363e27671ad1 \
	c2df2db47df74ed5790187b5179fb1995ed98222; do
	patch cockroachdb/cockroach $c
done
get etcd-io/raft $raft tla/README.md
get etcd-io/raft $raft tla/MCetcdraft.cfg
get etcd-io/raft $raft tla/validate-model.sh
get ceph/ceph $ceph doc/releases/emperor.rst
get ceph/ceph $ceph doc/releases/mimic.rst
for c in 97ed3fced13dd48520ec9c165537ff0bbc7cbb64 1e874403bf861cb8b74261308d8b73434cf90341 \
	31ed673a7484c8067dd928cabd5bcb923a35dbaf 0c611b362fb9cc4225f18283f74299551c2c5953 \
	eef1928e0153f09dc2522bb885b0ad9f5c65f4bd 71b5b39220b5f69995faffad1ef6992db6a337df; do
	patch ceph/ceph $c
done
get pingcap/tla-plus $pingcap README.md
get pingcap/tla-plus $pingcap DistributedTransaction/Test1.cfg
get mongodb-labs/vldb25-dist-txns $mongo check_models.sh
url aws-formal-methods-2014.pdf https://6826.csail.mit.edu/2019/papers/formal-methods-amazon.pdf
url mongodb-dist-txns-vldb2025.pdf https://www.vldb.org/pvldb/vol18/p5045-schultz.pdf
python3 -I "$here/pdftext.py" aws-formal-methods-2014.pdf | tr '\n' ' ' | sed 's/  */ /g' | fold -s -w 300 >aws-formal-methods-2014.txt
python3 -I "$here/pdftext.py" mongodb-dist-txns-vldb2025.pdf | tr -d '\n' | fold -w 300 >mongodb-dist-txns-vldb2025.txt

show() { # file pattern
	echo "== $1: $2"
	{ grep -n -E -- "$2" "$1" || echo "   (no match)"; } | head -6 | cut -c1-300
}

c=cockroachdb_cockroach_
show ${c}docs_tla-plus_ParallelCommits_ParallelCommits.tla "loss of information|\(#37866\)|\(#37784\)|^AckImpliesCommit ==|^ImplicitCommitLeadsToExplicitCommit =="
show ${c}docs_tla-plus_ParallelCommits_ParallelCommits.cfg "KEYS|PREVENTERS|MAX_ATTEMPTS"
show ${c}docs_tla-plus_ParallelCommits_ParallelCommits.toolbox_ParallelCommits___Model_1.launch "numberOfWorkers"
show ${c}docs_tla-plus_StoreLiveness_StoreLiveness.cfg "Nodes|MaxClock|MaxRestarts"
show ${c}pkg_kv_kvserver_storeliveness_doc.go "verified using TLA\+"
show cockroachdb_cockroach_50e10374283e37525610eda141082871bf2a5554.patch "overly strict assertion"
show cockroachdb_cockroach_74c36e51a5b3825076c378834558de234b13feae.patch "hazard discussed in #37866|unexpectedly already committed"
show cockroachdb_cockroach_8dc06b6a49fbf43b6e81baa2693e5c890a36e21b.patch "medium-term solution|still ambiguous"
show cockroachdb_cockroach_abc7f04791315c949bcd4fc13248ef41630eada3.patch "54130"
show cockroachdb_cockroach_c2df2db47df74ed5790187b5179fb1995ed98222.patch "Earlier we thought clock propagation|Support Disjointness"
show etcd-io_raft_tla_README.md "at least hours|-lncheck final"
show etcd-io_raft_tla_MCetcdraft.cfg "Server|Limit"
show etcd-io_raft_tla_validate-model.sh "nightly.tlapl.us"
show ceph_ceph_doc_releases_emperor.rst "disallow CephFS snapshots"
show ceph_ceph_doc_releases_mimic.rst "Snapshots are now stable|snapshot is now stable"
show ceph_ceph_97ed3fced13dd48520ec9c165537ff0bbc7cbb64.patch "critical|regression|interrupted"
show ceph_ceph_1e874403bf861cb8b74261308d8b73434cf90341.patch "IO was being unblocked"
show ceph_ceph_31ed673a7484c8067dd928cabd5bcb923a35dbaf.patch "^Subject"
show ceph_ceph_0c611b362fb9cc4225f18283f74299551c2c5953.patch "oldest_map can lag|^Subject"
show ceph_ceph_eef1928e0153f09dc2522bb885b0ad9f5c65f4bd.patch "Trimming past last_update_ondisk"
show ceph_ceph_71b5b39220b5f69995faffad1ef6992db6a337df.patch "possible for the renewal|crashing the replicas"
show pingcap_tla-plus_README.md "distributed transaction|Raft"
show mongodb-labs_vldb25-dist-txns_check_models.sh "workers"
show aws-formal-methods-2014.txt "35 high level|ten cc1.4xlarge|few seconds|2 to 3 weeks|Found 3 bugs"
show mongodb-dist-txns-vldb2025.txt "27-step|3m 20s|m6g.2xlarge|2,132,765"
