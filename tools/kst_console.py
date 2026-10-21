#!/usr/bin/env python3
"""표준 입력의 줄/진행 표시를 KST 시각과 함께 즉시 출력한다."""
import datetime
import os
import re
import sys

kst = datetime.timezone(datetime.timedelta(hours=9))
pending = b""
def emit(raw):
    if raw:
        stamp = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S KST")
        print(f"[{stamp}] {raw.decode('utf-8', errors='replace')}", flush=True)

while True:
    chunk = os.read(sys.stdin.fileno(), 65536)
    if not chunk:
        emit(pending)
        break
    lines = re.split(b"\r\n|\r|\n", pending + chunk)
    pending = lines.pop()
    for line in lines:
        emit(line)
