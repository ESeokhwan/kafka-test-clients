#!/usr/bin/env bash
# .42: 조회·검사 1초/5분, 여섯 delete/fio 조합, fio ON 회차는 한 번 쓰기.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$base/run_client42_comparison.sh" --intervals 1000,300000 --fio-passes 1 "$@"
