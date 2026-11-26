"""單家直接重播，多家各用獨立程序和輸出目錄。"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import subprocess
import sys

from common import configure_logging
from common.segments import utc_day
from .normalization import DEFAULT_BATCH_SIZE, REFERENCE_MAPPER, mapper_for, replay, time_window


def positive(value):
    # 工作程序和提交批量至少為一。
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError('expected a positive integer')
    return number


def paths_for(paths, families):
    # 單家沿用原本路徑；多家可傳共同父目錄，或依交易所順序逐一指定。
    if len(paths) == len(families):
        return [path.resolve() for path in paths]
    if len(paths) == 1:
        return [(paths[0] / family).resolve() for family in families]
    raise ValueError('provide one parent directory or one path per exchange')


def make_jobs(args):
    # 預先檢查重複交易所及輸出衝突，避免一個批次被執行兩次。
    families = args.exchange
    if len(families) != len(set(families)):
        raise ValueError('duplicate exchange')
    sources = paths_for(args.input, families)
    outputs = paths_for(args.output, families)
    if len(set(sources)) != len(sources):
        raise ValueError('each exchange needs its own input directory')
    for index, output in enumerate(outputs):
        if output.exists():
            raise ValueError(f'output directory already exists: {output}')
        if any(output.is_relative_to(other) or other.is_relative_to(output) for other in outputs[:index]):
            raise ValueError('exchange output directories overlap')
    return list(zip(families, sources, outputs))


def run_child(job, args):
    # 每家使用當前 Python 啟動一次；只回傳小摘要，退出程序即可回收整個位址空間。
    family, source, output = job
    script = Path(__file__).with_name('02_normalize_tardis.py')
    command = [sys.executable, str(script), '--exchange', family, '--input', str(source), '--output', str(output),
               '--batch-size', str(args.batch_size), '--worker-summary']
    for flag, value in (('--start-us', args.start_us), ('--end-us', args.end_us), ('--date', args.date)):
        if value is not None:
            command.extend([flag, str(value)])
    if args.recover:
        command.append('--recover')
    child = subprocess.run(command, stdout=subprocess.PIPE, text=True)
    if child.returncode:
        return family, dict(status='failed', returncode=child.returncode, output=str(output))
    return family, json.loads(child.stdout)


def main():
    # 同一組可省略的事件時間窗同時適用每個交易所，單家保留舊的 stdout 結構。
    configure_logging()
    parser = argparse.ArgumentParser(description='原始 JSONL 離線轉換為 Tardis CSV')
    parser.add_argument('--exchange', required=True, action='extend', nargs='+', choices=sorted(REFERENCE_MAPPER))
    parser.add_argument('--input', required=True, action='extend', nargs='+', type=Path)
    parser.add_argument('--output', required=True, action='extend', nargs='+', type=Path)
    parser.add_argument('--jobs', type=positive, default=2, help='多家交易所同時執行的程序上限（預設 2）')
    parser.add_argument('--batch-size', type=positive, default=DEFAULT_BATCH_SIZE, help='每批提交的原始紀錄數')
    parser.add_argument('--recover', action='store_true')
    parser.add_argument('--date', type=utc_day, help='UTC 事件日期 YYYY-MM-DD，可處理採集中的已封段範圍')
    parser.add_argument('--start-us', type=int, help='事件時間下限（含），單位為 epoch 微秒')
    parser.add_argument('--end-us', type=int, help='事件時間上限（不含），單位為 epoch 微秒')
    parser.add_argument('--worker-summary', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        time_window(args.start_us, args.end_us, args.date)
        jobs = make_jobs(args)
    except ValueError as exc:
        parser.error(str(exc))
    if len(jobs) == 1:
        family, source, output = jobs[0]
        result = replay(mapper_for(family), source, output, args.recover, args.start_us, args.end_us, args.date,
                        batch_size=args.batch_size, result='compact' if args.worker_summary else 'console')
        if args.worker_summary:
            print(json.dumps(result))
        return
    with ThreadPoolExecutor(max_workers=min(args.jobs, len(jobs))) as pool:
        results = dict(pool.map(lambda job: run_child(job, args), jobs))
    print(json.dumps(results, ensure_ascii=False))
    if any(result['status'] == 'failed' for result in results.values()):
        raise SystemExit(1)
