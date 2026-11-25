"""串流分析 Tardis CSV，固定排除 sample，逐 exchange/symbol 輸出 Markdown 報告。

python tardis_sample/step1/analyze_dataset_csv.py
python tardis_sample/step1/analyze_dataset_csv.py --source tardis_sample/datasets --output /tmp/dataset_reports
python tardis_sample/step1/analyze_dataset_csv.py --pattern '*liquidations/*.csv'

時間為 UTC epoch 微秒。靜默為同 exchange/symbol 全部類型共同無訊息 >5 秒。
不含跨日及觀測範圍外區間；靜默按 timestamp 排序合併，倒序按原始列檢查。
"""

import argparse
import csv
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import gzip
import math
import json
import sqlite3
from urllib.parse import quote
import re
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parent
TIME_COLUMNS = ('timestamp', 'local_timestamp')
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
NUMERIC_COLUMNS = {'price', 'amount', 'ask_price', 'bid_price', 'ask_amount', 'bid_amount',
                   'funding_rate', 'predicted_funding_rate', 'open_interest',
                   'last_price', 'index_price', 'mark_price'}


def cell(value):
    return str(value if value is not None else '—').replace('|', '\\|').replace('\n', ' ')


def table(stream, headings, rows, trailing_blank=True):
    stream.write('| ' + ' | '.join(headings) + ' |\n')
    stream.write('| ' + ' | '.join(['---'] * len(headings)) + ' |\n')
    for row in rows:
        stream.write('| ' + ' | '.join(map(cell, row)) + ' |\n')
    if trailing_blank:
        stream.write('\n')


class Activity:
    """將每個訊息的 [t, t+門檻] 合併為區段，磁碟暫存而非保留所有時間。"""
    def __init__(self, db, date, threshold, enabled=True):
        self.db, self.date, self.threshold = db, date, threshold
        self.enabled = enabled
        self.start = self.end = None

    def add(self, value):
        if not self.enabled:
            return
        end = value + self.threshold
        if self.start is not None and value <= self.end and end >= self.start:
            self.start = min(self.start, value)
            self.end = max(self.end, end)
        else:
            self.flush()
            self.start, self.end = value, end

    def flush(self):
        if self.start is not None:
            self.db.execute('INSERT INTO activity VALUES (?, ?, ?)', (self.date, self.start, self.end))
        self.start = self.end = None


def required_types(exchange):
    required = {'trades', 'quotes', 'book_ticker'}
    if exchange in {'binance-futures', 'bybit', 'okex-swap'}:
        required |= {'derivative_ticker', 'liquidations'}
    elif exchange == 'upbit':
        required.remove('book_ticker')
    return required


def write_silence(stream, db, results, threshold):
    required = required_types(results[0]['exchange'])
    by_date = defaultdict(list)
    for r in results:
        by_date[r['date']]  # 保留沒有任何必要類型的日期
        if r['data_type'] in required:
            by_date[r['date']].append(r)
    stream.write('使用 timestamp 合併訊息（Upbit 兩類、其他現貨三類、永續五類）；起點為最後一筆訊息、終點為下一筆訊息，兩端不包含在靜默內。\n\n')
    for date, entries in sorted(by_date.items()):
        stream.write(f'### {date}\n\n')
        missing = required - {r['data_type'] for r in entries}
        bad = [r['file'] for r in entries if r['status'] != 'empty_file' and (r['status'] == 'error' or r['malformed_rows']
               or 'timestamp' not in r['times']
               or r['times']['timestamp']['empty'] or r['times']['timestamp']['invalid'])]
        if missing or bad or date == 'unknown':
            stream.write('無法判定共同靜默；不以未讀取的資料當作沒有訊息。\n\n')
            if missing:
                stream.write('缺少類型：' + ', '.join(sorted(missing)) + '\n\n')
            if bad:
                stream.write('資料不完整：' + ', '.join(bad) + '\n\n')
            continue
        count = 0
        end = None
        table(stream, ['開始 UTC', '結束 UTC', '開始 epoch µs', '結束 epoch µs', '共同靜默秒'], [], trailing_blank=False)
        for start, current_end in db.execute('SELECT start, end FROM activity WHERE date=? ORDER BY start', (date,)):
            if end is not None and start > end:
                last_message = end - threshold
                stream.write('| ' + ' | '.join(map(cell, [utc(last_message), utc(start), last_message, start,
                                                        f'{(start - last_message) / 1_000_000:.6f}'])) + ' |\n')
                count += 1
            end = current_end if end is None else max(end, current_end)
        stream.write(f'\n共同靜默區間數：**{count}**。\n\n' if end is not None else '\n無有效訊息，無法建立觀測範圍。\n\n')


def utc(value):
    if value is None:
        return ''
    try:
        return (EPOCH + timedelta(microseconds=value)).isoformat()
    except OverflowError:
        return ''


def analyze_file(path, label, threshold_us, activity):
    result = dict(file=label, bytes=path.stat().st_size, status='ok', error='',
                  rows=0, valid_rows=0, malformed_rows=0, columns=[], fields={}, times={})
    opener = gzip.open if path.name.endswith('.gz') else open
    previous_times = {}
    try:
        with opener(path, 'rt', encoding='utf-8-sig', newline='') as stream:
            reader = csv.reader(stream, strict=True)
            header = next(reader, None)
            if header is None:
                result['status'] = 'empty_file'
                return result
            if not header or any(not name for name in header) or len(set(header)) != len(header):
                raise ValueError('缺少表頭、空欄名或重複欄名')
            result['columns'] = header
            fields = result['fields'] = {
                name: dict(empty=0, nonempty=0, numeric_count=0,
                           numeric_min=None, numeric_max=None) for name in header
            }
            positions = {name: header.index(name) for name in TIME_COLUMNS if name in header}
            times = result['times'] = {
                name: dict(valid=0, empty=0, invalid=0, first_us=None, last_us=None,
                           min_us=None, max_us=None,
                           max_forward_interval_us=0)
                for name in positions
            }
            if 'timestamp' in times:
                times['timestamp']['backwards'] = 0
            for record, row in enumerate(reader, start=1):
                result['rows'] += 1
                if len(row) != len(header):
                    result['malformed_rows'] += 1
                    previous_times.clear()
                    continue
                result['valid_rows'] += 1
                for index, (name, value) in enumerate(zip(header, row)):
                    fields[name]['empty' if not value.strip() else 'nonempty'] += 1
                    if name in NUMERIC_COLUMNS and value.strip():
                        try:
                            number = float(value)
                            if math.isfinite(number):
                                stat = fields[name]
                                stat['numeric_count'] += 1
                                stat['numeric_min'] = number if stat['numeric_min'] is None else min(stat['numeric_min'], number)
                                stat['numeric_max'] = number if stat['numeric_max'] is None else max(stat['numeric_max'], number)
                        except ValueError:
                            pass
                for name, index in positions.items():
                    stat = times[name]
                    raw = row[index].strip()
                    if not raw:
                        stat['empty'] += 1
                        previous_times.pop(name, None)
                        continue
                    try:
                        value = int(raw)
                    except ValueError:
                        stat['invalid'] += 1
                        previous_times.pop(name, None)
                        continue
                    if name == 'timestamp':
                        activity.add(value)
                    stat['valid'] += 1
                    if stat['first_us'] is None:
                        stat['first_us'] = value
                        stat['min_us'] = stat['max_us'] = value
                    stat['last_us'] = value
                    stat['min_us'] = min(stat['min_us'], value)
                    stat['max_us'] = max(stat['max_us'], value)
                    if name in previous_times:
                        old, old_record = previous_times[name]
                        delta = value - old
                        if name == 'timestamp' and delta < 0:
                            stat['backwards'] += 1
                        stat['max_forward_interval_us'] = max(stat['max_forward_interval_us'], delta)
                    previous_times[name] = value, record
    except (OSError, EOFError, UnicodeError, csv.Error, ValueError) as exc:
        result['status'] = 'error'
        result['error'] = str(exc)
    if result['status'] == 'ok':
        if result['malformed_rows'] or any(s['invalid'] or s.get('backwards', 0) for s in result['times'].values()):
            result['status'] = 'warning'
        elif result['rows'] == 0:
            result['status'] = 'header_only'
    for stat in result['fields'].values():
        stat['empty_ratio'] = stat['empty'] / result['valid_rows'] if result['valid_rows'] else None
    for stat in result['times'].values():
        for key in ('first', 'last', 'min', 'max'):
            stat[key + '_utc'] = utc(stat[key + '_us'])
    return result

def identify(result):
    """使用既有 exchange/data_type/YYYY-MM-DD_symbol 命名，未知格式不猜測。"""
    path = Path(result['file'])
    match = re.match(r'(\d{4}-\d{2}-\d{2})_(.+)\.csv(?:\.gz)?$', path.name)
    exchange, kind = path.parts[:2] if len(path.parts) >= 3 else ('unknown', 'unknown')
    date, symbol = match.groups() if match else ('unknown', path.name)
    tokens = re.split(r'[-_/]', symbol.upper())
    asset = next((a for a in ('BTC', 'ETH', 'SOL', 'GRT')
                  if a in tokens or symbol.upper() in (a + 'USDT', a + 'USD')), 'unknown')
    result.update(exchange=exchange, data_type=kind, date=date, symbol=symbol, asset=asset)


def comparison(stream, results, binance_only):
    groups = defaultdict(list)
    for r in results:
        if r['asset'] == 'unknown' or binance_only and r['exchange'] != 'binance-futures':
            continue
        key = (r['date'], r['data_type']) if binance_only else (r['date'], r['asset'], r['data_type'])
        groups[key].append(r)
    if not groups:
        stream.write('本次篩選沒有適用資料。\n\n')
    for key, entries in sorted(groups.items()):
        stream.write('### ' + ' / '.join(key) + '\n\n')
        rows = []
        for r in entries:
            ts = r['times'].get('timestamp', {})
            local = r['times'].get('local_timestamp', {})
            span = ((local['max_us'] - local['min_us']) / 1_000_000
                    if local.get('min_us') is not None else 0)
            rows.append([r['exchange'], r['symbol'], r['status'], r['rows'],
                         f'{r["valid_rows"] / span:.4f}' if span > 0 else '—',
                         ts.get('backwards', '—')])
        table(stream, ['市場', 'symbol', '狀態', '列數', '有效列/觀測秒',
                       'timestamp 倒序'], rows)
        usable = [r for r in entries if r['status'] != 'error']
        if len(usable) >= 2:
            low, high = min(usable, key=lambda r: r['rows']), max(usable, key=lambda r: r['rows'])
            ratio = f'{high["rows"] / low["rows"]:.2f} 倍' if low['rows'] else '最小值為零，無法計算倍數'
            stream.write(f'列數最大：{high["exchange"]}/{high["symbol"]}（{high["rows"]}）；'
                         f'最小：{low["exchange"]}/{low["symbol"]}（{low["rows"]}）；{ratio}。\n\n')
        schemas = {tuple(r['columns']) for r in usable}
        stream.write(f'非 error 檔案包含 {len(schemas)} 種 schema；缺少的欄位以下以 — 表示。\n\n')
        field_rows = []
        for name in sorted({c for r in entries for c in r['fields']} - {'exchange', 'symbol', *TIME_COLUMNS}):
            for r in entries:
                s = r['fields'].get(name)
                field_rows.append([name, r['exchange'], r['symbol'],
                                   f'{s["empty_ratio"]:.2%}' if s and s['empty_ratio'] is not None else '—',
                                   s['numeric_min'] if s else '—',
                                   s['numeric_max'] if s else '—'])
        table(stream, ['欄位', '市場', 'symbol', '空值率', '數值最小', '數值最大'], field_rows)


def write_report(stream, results, gaps, source, threshold):
    stream.write('# CSV 數據統計與間隔分析報告\n\n')
    generated_at = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec='seconds')
    stream.write(f'生成時間：{generated_at}（UTC+8）\n\n')
    stream.write(f'分析 {len(results)} 份 CSV；固定排除 sample。間隔門檻：嚴格大於 {threshold / 1_000_000:g} 秒。\n\n')
    stream.write('## 解讀方式\n\n'
                 '- 最早／最晚是全檔有效時間的 min/max，不是首末列；兩種 timestamp 分開計算。\n'
                 '- 靜默按同 exchange/symbol/date 的全部必要類型合併 timestamp 判定（Upbit 兩類、其他現貨三類、永續五類），任一類型有訊息即中斷。\n'
                 '- 倒序只檢查 timestamp：後一筆小於前一筆時計一次；local_timestamp 顯示「未檢查」。\n'
                 '- 靜默不含觀測範圍外的日首／日尾或跨日區間；缺檔或時間資料不完整的日期不判定。\n'
                 '- error 檔案的結果可能不完整；warning 表示格式、時間解析或 timestamp 倒序異常。\n'
                 '- 比較按同日期及 data type 分組；缺席的市場／幣種不代表零筆。\n'
                 '- 有效列/觀測秒以 local_timestamp 範圍計算，不能當成全天頻率。\n'
                 '- 數值範圍使用有限浮點數，供描述性比較，非精確財務加總。\n'
                 '- 跨市場的 spot/perpetual、USD/USDT/KRW、合約數量單位不同；價格和數量範圍未換算。\n'
                 '- 這是分布與完整性比較，沒有逐筆事件配對或判定哪個 feed 先到；靜默不直接代表丟包。\n\n')
    stream.write('## 每份 CSV 概覽\n\n')
    table(stream, ['檔案', '狀態', '列數', '有效列', '欄位數錯誤', '錯誤訊息'],
          ([r['file'], r['status'], r['rows'], r['valid_rows'], r['malformed_rows'], r['error']] for r in results))
    stream.write('本報告僅含當前 exchange/symbol；跨幣種與跨市場比較於全部掃描完成後補入。\n\n')
    stream.write('## 逐檔 UTC 時間範圍與欄位統計\n\n')
    for r in results:
        stream.write(f'### {r["file"]}\n\n')
        table(stream, ['時間欄位', '最早 UTC', '最晚 UTC', '最大正向間隔秒', '倒序', '空值', '非法'],
              ([name, s['min_utc'], s['max_utc'], s['max_forward_interval_us'] / 1_000_000,
                s.get('backwards', '未檢查'), s['empty'], s['invalid']] for name, s in r['times'].items()))
        missing = set(TIME_COLUMNS) - r['times'].keys()
        if missing:
            stream.write('缺少時間欄位：' + ', '.join(sorted(missing)) + '\n\n')
        table(stream, ['欄位', '空值', '非空', '空值率', '有限數值數', '最小', '最大'],
              ([name, s['empty'], s['nonempty'], f'{s["empty_ratio"]:.2%}' if s['empty_ratio'] is not None else '—',
                s['numeric_count'], s['numeric_min'], s['numeric_max']]
               for name, s in r['fields'].items()))
    stream.write('## 全部資料類型共同靜默區間\n\n')
    gaps.seek(0)
    shutil.copyfileobj(gaps, stream)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--source', type=Path, default=BASE_DIR.parent / 'datasets_extracted')
    parser.add_argument('--output', type=Path, default=BASE_DIR / 'dataset_reports', help='報告根目錄，輸出 exchange/symbol.md')
    parser.add_argument('--pattern', default='*')
    parser.add_argument('--empirical', action='store_true', help='Run full-day BBO, field-change and gap analysis (requires duckdb)')
    parser.add_argument('--date', default='2026-09-18')
    parser.add_argument('--gap-seconds', default='5')
    args = parser.parse_args()
    if args.empirical:
        from step1_empirical import run
        run(args.source, args.output / "empirical", args.date)
        return 0
    try:
        threshold = Decimal(args.gap_seconds) * 1_000_000
        if not threshold.is_finite() or threshold <= 0 or threshold != threshold.to_integral_value():
            raise ValueError()
        threshold = int(threshold)
    except (InvalidOperation, ValueError):
        parser.error('--gap-seconds 必須是正數，精度最多到微秒')
    source, output = args.source.resolve(), args.output.resolve()
    if not source.is_dir():
        parser.error('來源資料夾不存在')
    if output.is_file() or output.suffix.lower() == '.md':
        parser.error('--output 必須為資料夾，不是 Markdown 檔案')
    files = [p for p in sorted(source.rglob('*')) if p.is_file()
             and (p.name.endswith('.csv') or p.name.endswith('.csv.gz'))
             and not re.search(r'(^|[_\-.])sample([_\-.]|$)', p.name, re.IGNORECASE)
             and p.relative_to(source).match(args.pattern)]
    if not files:
        parser.error('沒有符合條件的完整 CSV')
    # 若同一相對路徑同時有 csv 與 csv.gz，優先未壓縮版，避免雙計。
    selected = set(files)
    files = [p for p in files if not (p.suffix == '.gz' and p.with_suffix('') in selected)]
    groups = defaultdict(list)
    for path in files:
        identity = {'file': path.relative_to(source).as_posix()}
        identify(identity)
        groups[(identity['exchange'], identity['symbol'])].append(path)
    output.mkdir(parents=True, exist_ok=True)
    failures = 0
    processed = 0
    # 磁碟暫存只存統計摘要；不保留其他 symbol 的列資料或 gap。
    with tempfile.TemporaryDirectory(prefix='tardis-analysis-') as scratch:
        with sqlite3.connect(str(Path(scratch) / 'summaries.sqlite')) as db:
            db.execute('CREATE TABLE stats (exchange TEXT, symbol TEXT, date TEXT, kind TEXT, asset TEXT, payload TEXT)')
            db.execute('CREATE INDEX peers ON stats(date, kind, asset)')
            db.execute('CREATE TABLE activity (date TEXT, start INTEGER, end INTEGER)')
            for (exchange, symbol), paths in sorted(groups.items()):
                destination = output / quote(exchange, safe='') / (quote(symbol, safe='') + '.md')
                destination.parent.mkdir(parents=True, exist_ok=True)
                results = []
                temporary = None
                try:
                    with tempfile.TemporaryFile(mode='w+', encoding='utf-8') as gaps:
                        db.execute('DELETE FROM activity')
                        for path in paths:
                            processed += 1
                            print(f'[{processed}/{len(files)}] {path.relative_to(source)}', flush=True)
                            identity = {'file': path.relative_to(source).as_posix()}
                            identify(identity)
                            activity = Activity(db, identity['date'], threshold,
                                                identity['data_type'] in required_types(exchange))
                            r = analyze_file(path, identity['file'], threshold, activity)
                            activity.flush()
                            identify(r)
                            results.append(r)
                            failures += r['status'] == 'error'
                            db.execute('INSERT INTO stats VALUES (?, ?, ?, ?, ?, ?)',
                                       (r['exchange'], r['symbol'], r['date'], r['data_type'], r['asset'], json.dumps(r)))
                        db.commit()
                        write_silence(gaps, db, results, threshold)
                        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=destination.parent,
                                                         prefix='.report-', delete=False) as report:
                            temporary = Path(report.name)
                            write_report(report, results, gaps, source, threshold)
                        temporary.replace(destination)
                    print(f'已生成：{destination}', flush=True)
                finally:
                    if temporary:
                        temporary.unlink(missing_ok=True)
                    results.clear()
            # 其他 symbol 尚未掃描時無法比較；完成後逐組從磁碟載入摘要並補入既有報告。
            for exchange, symbol in sorted(groups):
                destination = output / quote(exchange, safe='') / (quote(symbol, safe='') + '.md')
                with destination.open('a', encoding='utf-8') as report:
                    report.write('\n\n## 比較結果（全批次掃描完成）\n\n')
                    keys = db.execute('SELECT DISTINCT date, kind, asset FROM stats WHERE exchange=? AND symbol=?',
                                      (exchange, symbol)).fetchall()
                    for date, kind, asset in keys:
                        if asset == 'unknown':
                            continue
                        if exchange == 'binance-futures':
                            report.write('### Binance Futures 幣種間比較\n\n')
                            peers = [json.loads(row[0]) for row in db.execute(
                                "SELECT payload FROM stats WHERE exchange='binance-futures' AND date=? AND kind=?",
                                (date, kind))]
                            comparison(report, peers, True)
                            del peers
                        report.write('### 跨市場同幣種比較\n\n')
                        peers = [json.loads(row[0]) for row in db.execute(
                            'SELECT payload FROM stats WHERE date=? AND kind=? AND asset=?', (date, kind, asset))]
                        comparison(report, peers, False)
                        del peers
    print(f'完成：{len(files)} 份 CSV，{len(groups)} 份 symbol 報告；輸出：{output}', flush=True)
    return int(failures > 0)


if __name__ == '__main__':
    sys.exit(main())
