#!/usr/bin/env bash
# .42에서 delete_on_fio_off 케이스를 실행한다. 추가 옵션은 공통 실행기로 전달한다.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$base/run_client42_comparison.sh" --variant delete_on_fio_off "$@"
