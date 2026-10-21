#!/usr/bin/env bash
# .42 전용: 삭제 IMMEDIATE / fio OFF.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$base/run_client42_comparison.sh" --variant delete_immediate_fio_off "$@"
