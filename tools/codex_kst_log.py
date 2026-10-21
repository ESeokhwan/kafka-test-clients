#!/usr/bin/env python3
"""tmux 출력의 각 줄에 KST 시간을 붙여 추가 저장한다."""
import argparse
import datetime
import os
import re
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
args = parser.parse_args()
kst = datetime.timezone(datetime.timedelta(hours=9), "KST")
ansi = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07]*(?:\x07|\x1b\\)")
os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
with open(args.output, "a", encoding="utf-8", buffering=1) as log:
    def emit(raw):
        value = ansi.sub("", raw.decode("utf-8", errors="replace")).rstrip("\r\n")
        stamp = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S.%f KST")
        log.write(f"[{stamp}] {value}\n")

    pending = b""
    while True:
        chunk = os.read(sys.stdin.fileno(), 65536)
        if not chunk:
            if pending:
                emit(pending)
            break
        pending += chunk
        # 터미널의 진행 표시(캐리지 리턴)도 각각 기록한다.
        lines = re.split(b"\r\n|\r|\n", pending)
        pending = lines.pop()
        for line in lines:
            if line:
                emit(line)
