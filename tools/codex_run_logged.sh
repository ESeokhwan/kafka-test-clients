#!/usr/bin/env bash
# codex tmux 안에서 작업 파일을 실행한다. -x는 실행 명령을 출력하므로 자격증명을 인수로 넣지 않는다.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ $# -eq 1 && -f "$1" ]] || { echo 'Usage: codex_run_logged.sh JOB_SCRIPT'; exit 2; }
[[ -n "${TMUX:-}" && "$(tmux display-message -p -t "$TMUX_PANE" '#S')" == codex ]] || { echo 'Run inside the codex tmux session.'; exit 2; }
[[ "$(tmux display-message -p -t "$TMUX_PANE" '#{pane_pipe}')" == 1 ]] || { echo 'tmux output logger is not attached.'; exit 2; }
job="$(readlink -f -- "$1")"
stamp() { TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S KST'; }
printf '[%s] JOB START %s\n' "$(stamp)" "$job"
sha256sum -- "$job"
set +e
# pipe-pane가 명령(-x)과 stdout/stderr 전체를 KST 시각과 함께 기록한다.
bash -Eeuxo pipefail "$job"
status=$?
set -e
printf '[%s] JOB END exit_code=%s %s\n' "$(stamp)" "$status" "$job"
printf '[%s] exit_code=%s\n' "$(stamp)" "$status" > "${job}.exit"
exit "$status"
