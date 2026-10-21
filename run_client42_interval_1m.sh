#!/usr/bin/env bash
# .42: 조회·비활성 검사를 모두 60000ms로 설정하고 여섯 delete/fio 조합 실행.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$base/run_client42_comparison.sh" --intervals 60000 "$@"
