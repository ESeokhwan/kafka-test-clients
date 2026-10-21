#!/usr/bin/env bash
# 전체 순회·실패 재시도·최종 CSV 수집을 한 진입점에서 관리한다.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python_bin="$(command -v python3 || command -v python)"
exec "$python_bin" -u "$base/tools/run_experiment_suite.py" "$@"
