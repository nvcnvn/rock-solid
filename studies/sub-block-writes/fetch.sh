#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl and git.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

ceph=b7e6787889ed0783df44063506dcacaaf672758f
linux=v6.18
zfs=1e9b428dcad3716cf11005dd8970bc91c8c0100c
juicefs=506808b0c521984e1780ff6ae8ca05cc0caddfaf
seaweedfs=d3ee817b2e9ff1ee441019a90d2b778754afef4b
lustre=ee61b3f6dd57dd642c14469fcb9593cd20e7a81d
vitastor=df593badb5dfdbfbacac97b0c310f6ea3d5c3dff
xfstests=22348afe338c0f6d540c0d7ef0db749eaa51b218
qemu=a19f59a72d96056d9a8f48cf6c77f5787491e414
kubevirt=e2a5aa2d49019b63b7540ce8ecb9d9eaaad02421
sqlite=58626f682afbc2e3467e8c2e5b37f7f29d89ed99

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

commit() { # repo sha: prints a commit's message
	local d
	d=$(echo "$1" | tr / _).git
	[ -d "$d" ] || git init -q --bare "$d"
	git -C "$d" fetch -q --depth 1 --filter=blob:none "https://github.com/$1" "$2"
	git -C "$d" log -1 --format='== %H%n%an %ad%n%B' FETCH_HEAD
}

get ceph/ceph $ceph src/mon/FSCommands.cc
get ceph/ceph $ceph doc/cephfs/experimental-features.rst
get ceph/ceph $ceph src/common/options/global.yaml.in
get ceph/ceph $ceph doc/rados/configuration/bluestore-config-ref.rst
get ceph/ceph $ceph src/os/bluestore/BlueStore.cc
get ceph/ceph $ceph src/osd/ECExtentCache.h
get ceph/ceph $ceph src/osd/PrimaryLogPG.cc
get torvalds/linux $linux fs/ceph/file.c
get torvalds/linux $linux fs/ceph/addr.c
get torvalds/linux $linux fs/fuse/file.c
get torvalds/linux $linux fs/fuse/dir.c
get torvalds/linux $linux drivers/block/ublk_drv.c
get torvalds/linux $linux include/uapi/linux/ublk_cmd.h
get torvalds/linux $linux drivers/block/rbd.c
get torvalds/linux $linux fs/xfs/xfs_buf.c
get torvalds/linux $linux fs/btrfs/fs.h
get torvalds/linux $linux fs/f2fs/f2fs.h
get torvalds/linux $linux fs/ext4/inline.c
get openzfs/zfs $zfs include/sys/zil.h
get openzfs/zfs $zfs module/zfs/zil.c
get openzfs/zfs $zfs include/sys/spa.h
get juicedata/juicefs $juicefs pkg/meta/base.go
get juicedata/juicefs $juicefs docs/en/introduction/architecture.md
get seaweedfs/seaweedfs $seaweedfs weed/storage/types/needle_types.go
get seaweedfs/seaweedfs $seaweedfs weed/filer/filechunks.go
get lustre/lustre-release $lustre lustre/lod/lod_dev.c
get vitalif/vitastor $vitastor docs/config/layout-cluster.en.md
get vitalif/vitastor $vitastor docs/usage/nfs.en.md
get vitalif/vitastor $vitastor src/nfs/nfs_kv_write.cpp
get kdave/xfstests $xfstests ltp/fsx.c
get kdave/xfstests $xfstests tests/generic/029
get qemu/qemu $qemu block/io.c
get kubevirt/kubevirt $kubevirt staging/src/kubevirt.io/api/core/v1/schema.go
get kubevirt/kubevirt $kubevirt pkg/virt-launcher/virtwrap/converter/storage/disk.go
get sqlite/sqlite $sqlite src/wal.c

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -6
}

show ceph_ceph_src_mon_FSCommands.cc "Inline data support is deprecated"
show ceph_ceph_doc_cephfs_experimental-features.rst "deprecated since the Octopus|generally <2KB"
show torvalds_linux_fs_ceph_file.c "ceph_has_inline_data\(ci\)|ceph_uninline_data\(file\)"
show ceph_ceph_src_common_options_global.yaml.in "name: bluestore_(min_alloc_size|min_alloc_size_hdd|min_alloc_size_ssd|prefer_deferred_size_hdd|prefer_deferred_size_ssd)$|must first pass through the BlueStore"
show ceph_ceph_doc_rados_configuration_bluestore-config-ref.rst "Pacific release changed|space amplification that was experienced|strands roughly 9 KB|allocates 24 KB"
show ceph_ceph_src_os_bluestore_BlueStore.cc "^void BlueStore::_do_write_small\(|head_pad = p2phase|uint64_t head_read = p2phase|l_bluestore_write_penalty_read_ops"
show ceph_ceph_src_osd_ECExtentCache.h "small sequential writes|single read to be outstanding|never re-order IO"
show ceph_ceph_src_osd_PrimaryLogPG.cc "// missing object\?|// degraded object\?|wait_for_degraded_object\(head, op\)"
show torvalds_linux_fs_fuse_file.c "ia->write.in.size = count|^static ssize_t fuse_perform_write|inarg->size = size - inarg->offset|Got truncated off completely"
show torvalds_linux_fs_fuse_dir.c "oldsize != outarg.attr.size\)|truncate_pagecache\(inode, outarg.attr.size\)"
show torvalds_linux_drivers_block_ublk_drv.c "logical_bs_shift > PAGE_SHIFT|logical_block_size	= 1 << p->logical_bs_shift"
show torvalds_linux_include_uapi_linux_ublk_cmd.h "__u8	logical_bs_shift|__u8	io_min_shift"
show torvalds_linux_drivers_block_rbd.c "\.io_min			= rbd_dev->opts->alloc_size|\.io_opt			= objset_bytes"
show torvalds_linux_fs_xfs_xfs_buf.c "bdev_validate_blocksize|Cannot use blocksize"
show torvalds_linux_fs_btrfs_fs.h "define BTRFS_DEFAULT_MAX_INLINE"
show torvalds_linux_fs_f2fs_f2fs.h "define MAX_INLINE_DATA"
show torvalds_linux_fs_ext4_inline.c "define EXT4_MIN_INLINE_DATA_SIZE"
show openzfs_zfs_include_sys_zil.h "define	ZIL_MIN_BLKSZ|Writes are handled in three different ways|WR_COPIED,	/\* immediate|blkptr_t	lr_blkptr"
show openzfs_zfs_module_zfs_zil.c "^uint_t zfs_immediate_write_sz"
show openzfs_zfs_include_sys_spa.h "define	BPE_NUM_WORDS|define	BPE_PAYLOAD_SIZE"
show juicedata_juicefs_pkg_meta_base.go "maxCompactSlices  =|maxSlices         =|len\(ss\) >= 5|numSlices%100 == 99"
show juicedata_juicefs_docs_en_introduction_architecture.md "each slice represents a single continuous write|most recent written slice|merging all slices within the same chunk"
show seaweedfs_seaweedfs_weed_storage_types_needle_types.go "NeedlePaddingSize  ="
show seaweedfs_seaweedfs_weed_filer_filechunks.go "^func MergeIntoVisibles|visibles.InsertInterval"
show lustre_lustre-release_lustre_lod_lod_dev.c "lod_dom_stripesize_max_kb = "
show vitalif_vitastor_docs_config_layout-cluster.en.md "^## bitmap_granularity|Required virtual disk write alignment"
show vitalif_vitastor_docs_usage_nfs.en.md "small files are stored in large"
show vitalif_vitastor_src_nfs_nfs_kv_write.cpp "^void nfs_do_rmw|CAS failure - retry|Allocate as is"
show kdave_xfstests_ltp_fsx.c "Checks for mmap last-page zero fill|^check_eofpage|^pollute_eofpage|non-zero data past EOF"
show kdave_xfstests_tests_generic_029 "Test mapped writes against truncate"
show qemu_qemu_block_io.c "^bdrv_padding_rmw_read"
show kubevirt_kubevirt_staging_src_kubevirt.io_api_core_v1_schema.go "^type CustomBlockSize|MatchVolume \*FeatureState"
show kubevirt_kubevirt_pkg_virt-launcher_virtwrap_converter_storage_disk.go "if source.BlockSize == nil"
show sqlite_sqlite_src_wal.c "define WAL_FRAME_HDRSIZE|define WAL_HDRSIZE"

commit freebsd/freebsd-src e312493b37949666334be3b6959e4dce62407c35
commit torvalds/linux 5885ebda878b47c4b4602d4b0410cb4b282af024
