#!/bin/bash
# Boots the Ubuntu 26.04 cloud image under KVM as this user and runs
# run-vm-root.sh chrony|timesyncd as root inside it. No root on the host: needs
# only /dev/kvm. The guest writes to a throwaway snapshot (-snapshot), its clock
# is its own kvm-clock, and every step and slew happens in the guest kernel; the
# host's NTP state is printed before and after to show it unchanged. The guest
# reaches public NTP servers through QEMU's user-mode network, so its time
# source is unaffected by the guest. cloud-init gets its seed over HTTP from a
# server on the host's loopback (NoCloud through the SMBIOS serial).
#
#   ./run-vm.sh chrony    > results-vm-chrony.txt
#   ./run-vm.sh timesyncd > results-vm-timesyncd.txt
set -eu
cd "$(dirname "$0")"
. ./env.sh
mode=${1:?chrony or timesyncd}
ssh_port=10022
http_port=18080
mkdir -p work/seed
[ -f work/id ] || ssh-keygen -q -t ed25519 -N '' -C clock-slew -f work/id
printf 'instance-id: clock-slew-%s\nlocal-hostname: clock-slew\n' "$mode" > work/seed/meta-data
cat > work/seed/user-data <<EOF
#cloud-config
ssh_authorized_keys: ["$(cat work/id.pub)"]
bootcmd: [systemctl mask --now apt-daily.timer apt-daily-upgrade.timer unattended-upgrades.service]
EOF

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup EXIT
python3 -I -m http.server --bind 127.0.0.1 --directory work/seed $http_port >/dev/null 2>&1 &
pids+=($!)
echo "# host $(uname -sr), $(lscpu | sed -n 's/^Model name: *//p'), $(qemu-system-x86_64 --version | head -1), $(date -u +%Y-%m-%dT%H:%M:%SZ)"
bin/adjtimex-read | sed 's/^/# host before: /'
echo "# host clocksource: $(cat /sys/devices/system/clocksource/clocksource0/current_clocksource)"
TMPDIR=$PWD/work taskset -c 8-13 qemu-system-x86_64 -nodefaults -display none -monitor none \
	-serial file:work/console-$mode.log -L vendor/sysroot/usr/share/seabios \
	-L vendor/sysroot/usr/share/qemu -machine q35,accel=kvm -cpu host -smp 2 -m 2G \
	-drive file=vendor/ubuntu-26.04-minimal-cloudimg-amd64.img,format=qcow2,if=virtio,snapshot=on \
	-smbios "type=1,serial=ds=nocloud;s=http://10.0.2.2:$http_port/" \
	-netdev user,id=n0,hostfwd=tcp:127.0.0.1:$ssh_port-:22 \
	-device virtio-net-pci,netdev=n0,romfile= &
pids+=($!)

g() { ssh -q -i work/id -p $ssh_port -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	-o ConnectTimeout=5 ubuntu@127.0.0.1 "$@"; }
for _ in $(seq 120); do g true && break; sleep 2; done
g 'cloud-init status --wait >/dev/null; mkdir -p clock-slew/bin'
scp -q -i work/id -P $ssh_port -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	run-vm-root.sh vendor/systemd-timesyncd_*.deb ubuntu@127.0.0.1:clock-slew/
scp -q -i work/id -P $ssh_port -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
	bin/clockrate bin/adjtimex-read ubuntu@127.0.0.1:clock-slew/bin/
g "sudo I_AM_A_DISPOSABLE_VM=1 clock-slew/run-vm-root.sh $mode" 2>&1
g 'sudo systemctl poweroff' || true
wait "${pids[1]}" 2>/dev/null || true
bin/adjtimex-read | sed 's/^/# host after: /'
