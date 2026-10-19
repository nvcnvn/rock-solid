#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

pebble=a7b8321871e3e3c19be21eaf42a6aaa7e2d04f23
rocksdb=abeebd9630f11bd08c28b7bd43c7bdfc62050654
linux=05f7e89ab9731565d8a62e3b5d1ec206485eeb0b
netbsd=718643fa52416ad2facf807ecd53bf90ee7ecbc3
scylla=3f42f42a62add6d5e36eb2ea603ef2b58c09aa3d

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

get cockroachdb/pebble $pebble recovery.go
get cockroachdb/pebble $pebble open.go
get cockroachdb/pebble $pebble version_set.go
get cockroachdb/pebble $pebble obsolete_files.go
get cockroachdb/pebble $pebble wal/reader.go
get cockroachdb/pebble $pebble record/record.go
get facebook/rocksdb $rocksdb db/db_impl/db_impl_open.cc
get facebook/rocksdb $rocksdb db/version_edit_handler.cc
get facebook/rocksdb $rocksdb db/log_format.h
get facebook/rocksdb $rocksdb db/log_reader.cc
get facebook/rocksdb $rocksdb include/rocksdb/options.h
get torvalds/linux $linux include/linux/f2fs_fs.h
get torvalds/linux $linux fs/f2fs/node.h
get torvalds/linux $linux fs/f2fs/recovery.c
get torvalds/linux $linux fs/f2fs/segment.c
get torvalds/linux $linux fs/f2fs/checkpoint.c
get torvalds/linux $linux fs/nilfs2/recovery.c
get torvalds/linux $linux include/uapi/linux/nilfs2_ondisk.h
get NetBSD/src $netbsd sys/ufs/lfs/lfs.h
get NetBSD/src $netbsd sys/ufs/lfs/lfs_rfw.c
get scylladb/scylladb $scylla sstables/sstable_directory.cc
get scylladb/scylladb $scylla compaction/compaction.cc

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -8
}

p=cockroachdb_pebble_
r=facebook_rocksdb_
l=torvalds_linux_
n=NetBSD_src_sys_ufs_lfs_
s=scylladb_scylladb_
show ${p}recovery.go "wal.Scan\(|minUnflushedLogNum \{|walsObsolete = append|Ratchet rs.maxFilenumUsed"
show ${p}open.go "Replay any newer log files|strictWALTail :=|scanObsoleteFiles\(rs.ls"
show ${p}version_set.go "ve.NextFileNum = vs.nextFileNum|func \(vs \*versionSet\) markFileNumUsed"
show ${p}obsolete_files.go "func \(d \*DB\) scanObsoleteFiles|liveFileNums\[obj.DiskFileNum\]"
show ${p}wal_reader.go "tail of one physical WAL may be|h.SeqNum <= r.lastSeqNum"
show ${p}record_record.go "Log number \(4B\)\| Payload|logNum != r.logNum"
show ${r}db_db_impl_db_impl_open.cc "wal_files\[number\] = LogFileName|since it is older than min log to keep|previous incarnation may not have written|sequence id of log files are|This will do a full scan"
show ${r}db_version_edit_handler.cc "next_file_number_.store\(version_edit_params_.GetNextFile"
show ${r}db_log_format.h "kRecyclableHeaderSize ="
show ${r}db_log_reader.cc "Treat a record from a previous instance of the log as EOF|return kOldRecord"
show ${r}include_rocksdb_options.h "size_t recycle_log_file_num = 0"
show ${l}include_linux_f2fs_fs.h "MAX_ACTIVE_LOGS	16|cur_node_segno\[|cur_data_blkoff\[|__le64 cp_ver|next_blkaddr;"
show ${l}fs_f2fs_node.h "static inline bool is_recoverable_dnode|return cp_ver == cpver_of_node"
show ${l}fs_f2fs_recovery.c "curseg = CURSEG_I\(sbi, CURSEG_WARM_NODE\)|if \(!is_recoverable_dnode\(folio\)\)|blkaddr = next_blkaddr_of_node"
show ${l}fs_f2fs_segment.c "Should call f2fs_clear_prefree_segments after checkpoint|static void f2fs_wait_discard_bio|__punch_discard_cmd\(sbi, dc, blkaddr\)|f2fs_wait_discard_bio\(sbi, \*new_blkaddr\)"
show ${l}fs_f2fs_checkpoint.c "f2fs_clear_prefree_segments\(sbi, cpc\)"
show ${l}fs_nilfs2_recovery.c "pseg_start = nilfs->ns_last_pseg|seg_seq = nilfs->ns_last_seq|ss_seq\) != seg_seq|Off the trail|seg_seq\+\+"
show ${l}include_uapi_linux_nilfs2_ondisk.h "__le64 ss_seq;|__le64 ss_next;"
show ${n}lfs.h "ss_ident;|ss_serial;|dlfs_curseg;|dlfs_offset;|dlfs_serial;"
show ${n}lfs_rfw.c "!= nextserial\)|Incorrect fsid|monotonically increasing serial number|startserial = lfs_sb_getserial"
show ${s}sstables_sstable_directory.cc "case component_type::TemporaryTOC|from an SSTable with a Temporary TOC"
show ${s}compaction_compaction.cc "void maybe_replace_exhausted_sstables_by_sst|exhausted sstables will be deleted as soon as possible"
