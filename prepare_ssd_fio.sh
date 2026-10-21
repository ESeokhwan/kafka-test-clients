#!/usr/bin/env bash
# 매 실행 시 가용 공간을 다시 계산하고 채우기/덮어쓰기 완료 후 임시 파일을 삭제한다.
set -Eeuo pipefail

fail() { printf '오류: %s\n' "$*" >&2; exit 1; }
# 직접 실행할 때도 --옵션을 사용한다. 이전 위치 인자 호출은 호환용으로 허용한다.
fio_target=""
fio_size=auto
while [[ $# -gt 0 ]]; do
    case "${1%%=*}" in
        --help|-h)
            echo "사용법: $0 --target-dir /data4 [--size-gib auto] [--passes 3] [--reserve-gib 2] [--expected-disk nvme1n1]"
            exit 0 ;;
        --target-dir|--size-gib|--passes|--reserve-gib|--expected-disk)
            option="${1%%=*}"
            if [[ "$1" == *=* ]]; then value="${1#*=}"; shift
            else
                [[ $# -ge 2 && "$2" != --* ]] || fail "$option 값이 필요합니다."
                value="$2"; shift 2
            fi
            [[ -n "$value" ]] || fail "$option 값이 비어 있습니다."
            case "$option" in
                --target-dir) fio_target="$value" ;; --size-gib) fio_size="$value" ;;
                --passes) FIO_PASSES="$value" ;; --reserve-gib) RESERVE_GIB="$value" ;;
                --expected-disk) EXPECTED_DISK="$value" ;;
            esac ;;
        --*) fail "알 수 없는 옵션: $1" ;;
        *) [[ -z "$fio_target" && $# -le 2 ]] || fail "잘못된 위치 인자: $1"
           fio_target="$1"; fio_size="${2:-auto}"; shift "$#" ;;
    esac
done
[[ -n "$fio_target" ]] || fail "--target-dir를 지정하세요."
for tool in fio df findmnt lsblk readlink awk grep mktemp truncate rm rmdir; do
    command -v "$tool" >/dev/null || fail "$tool 명령이 필요합니다."
done
target_dir="$(readlink -f -- "$fio_target")"
requested_size="$fio_size"
reserve_gib="${RESERVE_GIB:-2}"
# 첫 채우기 1회 + 같은 영역 덮어쓰기 2회가 기본값이다.
passes="${FIO_PASSES:-3}"
expected_disk="${EXPECTED_DISK:-nvme1n1}"
[[ -d "$target_dir" && -w "$target_dir" ]] || fail "쓰기 가능한 대상 디렉터리가 필요합니다."
[[ "$reserve_gib" =~ ^[1-9][0-9]{0,5}$ ]] || fail "RESERVE_GIB는 1~999999 정수여야 합니다."
[[ "$passes" =~ ^[1-9][0-9]{0,2}$ ]] || fail "FIO_PASSES는 1~999 정수여야 합니다."
[[ "$requested_size" == auto || "$requested_size" =~ ^[1-9][0-9]{0,5}$ ]] || fail "크기는 auto 또는 1~999999 GiB입니다."
[[ "$expected_disk" =~ ^[a-zA-Z0-9._-]+$ ]] || fail "EXPECTED_DISK 형식이 잘못되었습니다."
source_display="$(findmnt -T "$target_dir" -nro SOURCE)"
filesystem_type="$(findmnt -T "$target_dir" -nro FSTYPE)"
block_source="${source_display%%[*}"
[[ "$block_source" == /dev/* ]] || fail "블록 장치 파일시스템이 아닙니다: $source_display"
# 전체 출력을 읽어 pipefail 환경에서 조기 파이프 종료를 피한다.
lsblk -srno NAME "$block_source" | grep -Fx "$expected_disk" >/dev/null || fail "예상 장치 $expected_disk 와 일치하지 않습니다."
gib=$((1024 * 1024 * 1024))
reserve_bytes=$((reserve_gib * gib))
free_bytes="$(df -B1 --output=avail "$target_dir" | awk 'NR == 2 {print $1}')"
[[ "$free_bytes" =~ ^[0-9]+$ ]] || fail "가용 공간을 계산할 수 없습니다."
# 70 GiB 상한을 없애고, 여유 공간을 제외한 용량을 128 KiB 단위로 내림한다.
test_bytes=$(((free_bytes - reserve_bytes) / 131072 * 131072))
[[ "$requested_size" == auto ]] || test_bytes=$((requested_size * gib))
(( test_bytes >= 131072 && test_bytes + reserve_bytes <= free_bytes )) || fail "시험 파일과 예약 공간을 확보할 수 없습니다."

# 고유 디렉터리를 원자적으로 만들어 기존 파일/심볼릭 링크와 충돌하지 않게 한다.
work_dir="$(mktemp -d "${target_dir%/}/.fio-precondition.XXXXXXXX")"
test_file="$work_dir/payload.bin"
fio_pid=""
cleanup() {
    local status=$?
    trap - EXIT INT TERM HUP
    # 자식 fio가 완전히 종료된 다음 파일을 지워 공간 반환을 보장한다.
    if [[ -n "$fio_pid" ]]; then
        kill -TERM "$fio_pid" 2>/dev/null || true
        wait "$fio_pid" 2>/dev/null || true
    fi
    rm -f -- "$test_file" || status=1
    rmdir -- "$work_dir" || status=1
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
# Btrfs No_COW는 파일 크기 설정 및 실제 쓰기 전에 적용한다.
truncate -s 0 -- "$test_file"
if [[ "$filesystem_type" == btrfs ]]; then
    command -v chattr >/dev/null || fail "Btrfs에서는 chattr가 필요합니다."
    chattr +C -- "$test_file" || fail "No_COW 설정에 실패했습니다."
fi
truncate -s "$test_bytes" -- "$test_file"
printf '대상=%s 장치=%s 파일크기=%s bytes 반복=%s 예약=%s GiB\n' "$target_dir" "$source_display" "$test_bytes" "$passes" "$reserve_gib"
# 크기 예약은 sparse 파일이며 실제 공간 채우기는 첫 fio 쓰기에서 수행된다.
for ((pass = 1; pass <= passes; pass++)); do
    printf '준비 쓰기 %s/%s\n' "$pass" "$passes"
    fio --name="precondition-pass-$pass" --filename="$test_file" \
        --allow_file_create=0 --rw=write --bs=128k --direct=1 \
        --ioengine=libaio --iodepth=32 --numjobs=1 --size="$test_bytes" \
        --io_size="$test_bytes" --fallocate=none --overwrite=1 \
        --refill_buffers=1 --end_fsync=1 --group_reporting=1 --eta=always &
    fio_pid=$!
    wait "$fio_pid"
    fio_pid=""
done
# EXIT 정리 처리에서 파일을 삭제하며, 정리 실패도 비정상 종료로 전달한다.
printf '준비 쓰기 완료: 임시 파일을 삭제한 뒤 종료합니다.\n'
