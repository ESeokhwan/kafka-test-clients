#!/usr/bin/env python3
"""종료된 회차의 파일명과 결과 경로를 확정한다. CSV/원본 로그 내용은 보존한다."""
import argparse
import csv
import datetime
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--directory', required=True)
parser.add_argument('--tag', required=True)
parser.add_argument('--index', required=True)
parser.add_argument('--status', required=True)
args = parser.parse_args()
directory = Path(args.directory).resolve()
old = args.tag
if Path(old).name != old or not old.endswith('_end_PENDING'):
    raise ValueError('Invalid running tag')
now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
new = old[:-len('_end_PENDING')] + now.strftime('_end_%Y%m%d_%H%M%S_KST')
files = sorted(p for p in directory.iterdir() if p.is_file() and p.name.startswith(old))
if not files:
    raise ValueError('No round artifacts to finalize')
pairs = [(p, p.with_name(new + p.name[len(old):])) for p in files]
if any(target.exists() for _, target in pairs):
    raise FileExistsError('Final artifact already exists')
# 실행 명령은 실제 실행 당시의 경로를 보존하고 최종 산출물 경로만 갱신한다.
metadata = directory / (old + '.run.txt')
path_keys = {'TAG', 'PRODUCER_CSV', 'DELETION_CSV', 'IOSTAT_LOG'}
for source, target in pairs:
    source.rename(target)
final_metadata = directory / (new + '.run.txt')
if final_metadata.exists():
    lines = final_metadata.read_text(encoding='utf-8').splitlines(keepends=True)
    lines = [line.replace(old, new) if line.partition('=')[0] in path_keys else line for line in lines]
    lines += [f'EXECUTION_TAG={old}\n', f'END_TIME_KST={now.isoformat()}\n', f'FINAL_EXIT_CODE={args.status}\n']
    for source, target in pairs:
        lines.append(f'ARTIFACT_RENAME original={source.name} final={target.name}\n')
    temporary = final_metadata.with_suffix('.finalizing')
    temporary.write_text(''.join(lines), encoding='utf-8')
    os.replace(temporary, final_metadata)
index = Path(args.index)
if index.exists():
    with index.open(newline='', encoding='utf-8') as handle:
        rows = list(csv.reader(handle, delimiter='\t'))
    for row in rows[1:]:
        if row and row[0] == old:
            for i in range(min(6, len(row))):
                row[i] = row[i].replace(old, new)
    temporary = index.with_suffix('.finalizing')
    with temporary.open('w', newline='', encoding='utf-8') as handle:
        csv.writer(handle, delimiter='\t', lineterminator='\n').writerows(rows)
    os.replace(temporary, index)
print(f'회차 파일명 확정: {directory}/{new}*', flush=True)
