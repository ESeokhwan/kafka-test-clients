#!/usr/bin/env bash
# SSH에서 최초 준비 또는 재접속 시 호출한다. 기존 codex 셸에 명령을 주입하지 않는다.
set -Eeuo pipefail
base="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$base/codex_logs" "$base/codex_jobs"
command -v tmux >/dev/null
command -v python3 >/dev/null
if ! tmux has-session -t =codex 2>/dev/null; then
    tmux new-session -d -s codex -n work -c "$base" 'bash --noprofile --norc -i'
fi
pane="$(tmux display-message -p -t codex '#{pane_id}')"
log_file="$(tmux show-options -qv -t codex @codex_log_file)"
if [[ -z "$log_file" ]]; then
    log_file="$base/codex_logs/codex-$(TZ=Asia/Seoul date +%Y%m%d-%H%M%S).log"
    tmux set-option -t codex @codex_log_file "$log_file"
fi
if [[ "$(tmux display-message -p -t "$pane" '#{pane_pipe}')" == 0 ]]; then
    printf -v logger 'python3 %q --output %q' "$base/tools/codex_kst_log.py" "$log_file"
    tmux pipe-pane -o -t "$pane" "$logger"
fi
printf '[%s] tmux logging ready: pane=%s log=%s\n' "$(TZ=Asia/Seoul date '+%Y-%m-%d %H:%M:%S KST')" "$pane" "$log_file" | tee -a "$log_file"
printf 'Attach with: tmux attach -t codex\n'
