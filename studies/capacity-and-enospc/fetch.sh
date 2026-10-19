#!/usr/bin/env bash
# Downloads every source file the study cites, at the commits it read, and prints the cited lines.
# Usage: ./fetch.sh [dir]   (default ./work). Needs curl.
set -euo pipefail
out=${1:-"$(dirname "$0")/work"}
mkdir -p "$out"
cd "$out"

ceph=dcd6d90fabcee78f697a55151112d49f234668a3
linux=v6.18
vitastor=df593badb5dfdbfbacac97b0c310f6ea3d5c3dff
longhorn=d1968651ccc4bf60c97f55b7dcafdf7e228b5462
crdb=d30c905fff79ef825adc96bcc647f1872a90f2ff
minio=7aac2a2c5b7c882e68c1ce017d8256be2feea27f
juicefs=506808b0c521984e1780ff6ae8ca05cc0caddfaf
k8s=7f9d9d6a3d6e48be4d8d261c9474618d7f8d303d
k8sweb=1fc565c2f557823a911f073dc577945230f27dd7
provisioner=0d12300b577bf0fd017c3ddea1f435669cc1cfdb
csispec=25b3f10249abe6033887ed5177e856b9f73b1105
lvm2=0b58c33cdc51d26087a6e69c2f19ed660a43e989

get() { # repo commit path
	local f
	f=$(echo "$1/$3" | tr / _)
	[ -s "$f" ] || curl -sSfL -o "$f" "https://raw.githubusercontent.com/$1/$2/$3"
}

for p in src/mon/PGMap.cc src/mon/OSDMonitor.cc src/osd/OSDMap.cc src/osd/PrimaryLogPG.cc \
	src/crush/CrushWrapper.cc src/client/Client.cc src/osdc/Objecter.cc src/common/options/global.yaml.in \
	doc/rados/operations/monitoring.rst doc/rados/operations/balancer.rst doc/cephfs/full.rst \
	doc/rados/troubleshooting/troubleshooting-osd.rst; do
	get ceph/ceph $ceph $p
done
for p in fs/ext4/inode.c fs/ext4/balloc.c fs/ext4/super.c fs/ext4/page-io.c fs/xfs/xfs_file.c fs/xfs/xfs_iomap.c \
	fs/xfs/xfs_mount.c fs/xfs/xfs_mount.h fs/xfs/xfs_zone_space_resv.c fs/xfs/xfs_sysfs.c fs/xfs/xfs_log.c \
	fs/btrfs/file.c fs/btrfs/inode.c fs/nfs/file.c fs/nfs/internal.h fs/nfs/fs_context.c fs/ceph/file.c \
	fs/ceph/super.c fs/ceph/quota.c net/ceph/osd_client.c fs/fuse/file.c fs/open.c mm/filemap.c \
	include/linux/pagemap.h Documentation/core-api/errseq.rst drivers/md/dm-thin.c \
	Documentation/admin-guide/device-mapper/thin-provisioning.rst drivers/block/ublk_drv.c block/blk-core.c \
	kernel/hung_task.c lib/Kconfig.debug drivers/nvme/host/core.c drivers/nvme/host/tcp.c; do
	get torvalds/linux $linux $p
done
get vitalif/vitastor $vitastor src/cmd/cli_pool_ls.cpp
get vitalif/vitastor $vitastor mon/lp_optimizer/lp_optimizer.js
get vitalif/vitastor $vitastor docs/config/client.en.md
get vitalif/vitastor $vitastor src/client/cluster_client.cpp
get longhorn/longhorn-manager $longhorn csi/controller_server.go
get longhorn/longhorn-manager $longhorn types/setting.go
get longhorn/longhorn-manager $longhorn scheduler/replica_scheduler.go
get cockroachdb/cockroach $crdb pkg/kv/kvserver/allocator/allocatorimpl/allocator_scorer.go
get cockroachdb/cockroach $crdb pkg/storage/ballast.go
get minio/minio $minio cmd/notification-summary.go
get minio/minio $minio cmd/object-api-utils.go
get juicedata/juicefs $juicefs pkg/vfs/writer.go
get juicedata/juicefs $juicefs pkg/meta/quota.go
get juicedata/juicefs $juicefs pkg/meta/redis.go
get juicedata/juicefs $juicefs docs/en/guide/cache.md
get kubernetes/kubernetes $k8s pkg/scheduler/framework/plugins/volumebinding/binder.go
get kubernetes/website $k8sweb content/en/docs/concepts/storage/storage-capacity.md
get kubernetes-csi/external-provisioner $provisioner pkg/capacity/capacity.go
get container-storage-interface/spec $csispec spec.md
get lvmteam/lvm2 $lvm2 man/lvmthin.7_main

show() { # file pattern
	echo "== $1: $2"
	grep -n -E -- "$2" "$1" | head -6
}

show ceph_ceph_src_mon_PGMap.cc "int64_t PGMap::get_rule_avail|proj = \(int64_t\)\(avail / |statfs.kb_avail = get_pool_free_space"
show ceph_ceph_src_crush_CrushWrapper.cc "ultimate object placement is subject to the"
show ceph_ceph_src_osd_OSDMap.cc "void OSDMap::get_full_pools|float OSDMap::pool_raw_used_rate"
show ceph_ceph_src_osd_PrimaryLogPG.cc "full, dropping request|full, replying to FULL_TRY|fail-safe full check failed"
show ceph_ceph_src_client_Client.cc "void Client::_handle_full_flag|FULL, purging for ENOSPC"
show ceph_ceph_src_common_options_global.yaml.in "name: (mon_osd_full_ratio|mon_osd_backfillfull_ratio|mon_osd_nearfull_ratio|osd_failsafe_full_ratio)$"
show ceph_ceph_doc_cephfs_full.rst "may not be seen until|may be discarded after"
show torvalds_linux_fs_ceph_file.c "CEPH_OSDMAP_NEARFULL|err = -ENOSPC"
show torvalds_linux_fs_ceph_super.c "ABORT_ON_FULL"
show torvalds_linux_net_ceph_osd_client.c "static int abort_on_full_fn|cluster is full \(osdmap FULL\)"
show torvalds_linux_fs_ext4_inode.c "static int ext4_da_reserve_space|static int ext4_nonda_switch|2 \* free_clusters < 3 \* dirty_clusters"
show torvalds_linux_fs_ext4_super.c "resv_clusters = min_t"
show torvalds_linux_fs_xfs_xfs_mount.c "8192ULL|^xfs_dec_freecounter"
show torvalds_linux_fs_xfs_xfs_zone_space_resv.c "XC_FREE_RTEXTENTS counts the user available|^xfs_zoned_reserve_available"
show torvalds_linux_fs_xfs_xfs_sysfs.c "\.name = \"ENOSPC\""
show torvalds_linux_fs_btrfs_inode.c "unlink and rmdir are special"
show torvalds_linux_fs_nfs_file.c "^nfs_file_flush|case -ENOSPC:"
show torvalds_linux_fs_fuse_file.c "^static int fuse_flush|break out of the loop on short write"
show torvalds_linux_mm_filemap.c "^int file_check_and_advance_wb_err"
show torvalds_linux_drivers_md_dm-thin.c "define NO_SPACE_TIMEOUT_SECS|error_if_no_space \? BLK_STS_NOSPC|lookup_result.shared && \(rw == WRITE\)"
show torvalds_linux_drivers_block_ublk_drv.c "res = errno_to_blk_status\(io->res\)|^static enum blk_eh_timer_return ublk_timeout"
show torvalds_linux_block_blk-core.c "critical space allocation"
show torvalds_linux_drivers_nvme_host_core.c "unsigned int nvme_io_timeout = |case NVME_SC_CAP_EXCEEDED"
show vitalif_vitastor_src_cmd_cli_pool_ls.cpp "Calculate max_avail"
show vitalif_vitastor_docs_config_client.en.md "^## client_retry_enospc"
show longhorn_longhorn-manager_csi_controller_server.go "overProvisionLimit :=|maximumVolumeSize = max"
show cockroachdb_cockroach_pkg_kv_kvserver_allocator_allocatorimpl_allocator_scorer.go "defaultMaxDiskUtilizationThreshold = |defaultRebalanceToMaxDiskUtilizationThreshold = "
show cockroachdb_cockroach_pkg_storage_ballast.go "Default to a 1% or 1GiB"
show minio_minio_cmd_notification-summary.go "Ignore parity disks"
show juicedata_juicefs_pkg_meta_quota.go "format.Capacity > 0"
show kubernetes_kubernetes_pkg_scheduler_framework_plugins_volumebinding_binder.go "Prefer MaximumVolumeSize"
show lvmteam_lvm2_man_lvmthin.7_main "queue writes for up to 60|Disabling timeouts"
