#!/usr/bin/env bash
# 명령을 모의 처리하여 실제 TRIM/sync 없이 대상 장치 보호를 검증한다.
set -Eeuo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
export TRACE="$(mktemp)"
readlink() { echo /tmp; }
findmnt() { if [[ "$*" == *SOURCE ]]; then echo /dev/nvme1n1p1; else echo /data4; fi; }
lsblk() { printf '%s\n' nvme1n1p1 nvme1n1; }
fstrim() { [[ "$*" == '-v -- /data4' ]]; echo trim >> "$TRACE"; }
sync() { echo sync >> "$TRACE"; }
sudo() { [[ "$1" == -n ]]; shift; "$@"; }
export -f readlink findmnt lsblk fstrim sync sudo
bash "$root/prepare_ssd_trim.sh" /data4 nvme1n1
[[ "$(tr '\n' ' ' < "$TRACE")" == 'sync trim ' ]]
if bash "$root/prepare_ssd_trim.sh" /data4 nvme0n1; then exit 1; fi
[[ "$(tr '\n' ' ' < "$TRACE")" == 'sync trim ' ]]
echo 'PASS: TRIM target validation, sync ordering, mismatched disk rejected'
