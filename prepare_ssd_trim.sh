#!/usr/bin/env bash
# 지정 경로가 놓인 파일시스템과 장치를 확인한 뒤 빈 영역을 TRIM한다.
set -Eeuo pipefail
target="${1:?대상 디렉터리가 필요합니다.}"
expected="${2:?예상 장치 이름이 필요합니다.}"
[[ $# == 2 ]] || exit 2
for tool in readlink findmnt lsblk fstrim sync; do
    command -v "$tool" >/dev/null || { echo "필요한 명령 없음: $tool" >&2; exit 1; }
done
target="$(readlink -f -- "$target")"
[[ -d "$target" ]] || { echo "대상 디렉터리 없음: $target" >&2; exit 1; }
[[ "$expected" =~ ^[a-zA-Z0-9._-]+$ ]] || exit 2
# fstrim은 디렉터리 단위가 아니라 해당 파일시스템 전체의 빈 공간에 적용된다.
source="$(findmnt -T "$target" -nro SOURCE)"
mount="$(findmnt -T "$target" -nro TARGET)"
source="${source%%[*}"
[[ "$source" == /dev/* ]] || { echo "블록 장치가 아닌 대상: $source" >&2; exit 1; }
devices="$(lsblk -srno NAME "$source")"
grep -Fx -- "$expected" <<< "$devices" >/dev/null || { echo "예상 장치 불일치: $expected ($source)" >&2; exit 1; }
echo "TRIM 대상: directory=$target mount=$mount source=$source expected_disk=$expected"
sync
# 권한이 없으면 입력을 기다리지 않고 실패하여 생산 단계 진입을 막는다.
if (( EUID == 0 )); then
    fstrim -v -- "$mount"
else
    sudo -n fstrim -v -- "$mount"
fi
