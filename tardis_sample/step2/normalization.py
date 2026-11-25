"""第 02 階段共用的離線重播、版本追蹤、價位索引與 CSV 輸出。"""
import argparse
from collections import Counter, OrderedDict
import csv
import hashlib
import heapq
import json
from pathlib import Path
import sqlite3
from common import SCHEMAS, LOG, atomic_json, configure_logging, dec, encode, utc
from raw import read_records, read_runs, check_count

COMMIT = 'a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73'
REFERENCE_MAPPER = {'binance': 'binance', 'bybit': 'bybit', 'okx': 'okex', 'coinbase': 'coinbase', 'upbit': 'upbit'}


def reference_metadata(family):
    # 記下實際使用的原碼版本與本地檔案雜湊，離線重播不需連線。
    root = Path(__file__).parent
    paths = [f'src/mappers/{REFERENCE_MAPPER[family]}.ts', 'src/mappers/mapper.ts',
             'src/computable/booksnapshot.ts', 'src/orderbook.ts']
    if family in ('upbit', 'coinbase'):
        paths.append('src/handy.ts')
    sources = {Path(p).name: {'url': f'https://github.com/tardis-dev/tardis-node/blob/{COMMIT}/{p}',
                              'sha256': digest(root / 'reference' / Path(p).name)} for p in paths}
    local = [f'02_normalize_tardis_{family}.py', 'normalization.py', 'common.py', 'raw.py', 'retention.py']
    if family != 'binance':
        local += ['market_mapping.py', f'collectors/{family}.py', 'replica_protocol.py']
    return {'commit': COMMIT, 'reviewed_on': '2026-11-25', 'sources': sources,
            'comparison_notes': 'reference/README.md', 'policy': 'tardis_mapping_with_strict_local_book_checks',
            'implementation_sha256': {p: digest(root / p) for p in local}}


def digest(path):
    # 以完整檔案位元組核對版本。
    return hashlib.sha256(path.read_bytes()).hexdigest()


class BookError(ValueError):
    """訂單簿無法可靠接合或已超出已知快照範圍。"""


class Side:
    def __init__(self, bid):
        # 買盤反向儲存價位，使兩側都能用最小堆積取得最佳價。
        self.values = {}
        self.heap = []
        self.sign = -1 if bid else 1

    def apply(self, levels):
        # 每檔數量都是絕對值，零數量刪除該價位。
        for price, amount in levels:
            price, amount = dec(price), dec(amount)
            if amount == 0:
                self.values.pop(price, None)
            else:
                if price not in self.values:
                    heapq.heappush(self.heap, self.sign * price)
                self.values[price] = amount
        if len(self.heap) > 2 * len(self.values) + 1000:
            self.heap = [self.sign * price for price in self.values]
            heapq.heapify(self.heap)

    def best(self):
        # 延遲清除已從價位字典刪除的堆積項目。
        while self.heap and self.sign * self.heap[0] not in self.values:
            heapq.heappop(self.heap)
        if not self.heap:
            raise BookError('empty_side')
        price = self.sign * self.heap[0]
        return price, self.values[price]


class Output:
    def __init__(self, root, types):
        # 限制同時開啟的檔案數，每家只建立其市場支援的表格。
        self.root = root
        self.types = types
        self.open = OrderedDict()
        self.created = set()
        self.counts = Counter()
        self.ranges = {}

    def ensure(self, market, symbol, day):
        # 先建立當日表頭，讓有採集來源但未發生清算的日期仍有清算檔案。
        for target in self.types[market]:
            rel = f'{market}/{target}/{day}_{symbol}.csv'
            if rel in self.created:
                continue
            path = self.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('x', newline='', encoding='utf-8') as handle:
                csv.writer(handle).writerow(SCHEMAS[target])
            self.created.add(rel)

    def write(self, target, row):
        # 依事件時間分檔，保留十進位精度，並分別記下最早及最晚時間。
        market, symbol = row['exchange'], row['symbol']
        timestamp = row['timestamp']
        day = utc(timestamp)
        self.ensure(market, symbol, day)
        rel = f'{market}/{target}/{day}_{symbol}.csv'
        if rel not in self.open:
            if len(self.open) >= 40:
                self.open.popitem(last=False)[1][0].close()
            handle = (self.root / rel).open('a', newline='', encoding='utf-8')
            self.open[rel] = handle, csv.DictWriter(handle, fieldnames=SCHEMAS[target])
        self.open.move_to_end(rel)
        self.open[rel][1].writerow({k: encode(row.get(k)) for k in SCHEMAS[target]})
        self.counts[rel] += 1
        low, high = self.ranges.get(rel, (timestamp, timestamp))
        self.ranges[rel] = min(low, timestamp), max(high, timestamp)

    def close(self):
        # 將所有剩餘輸出緩衝寫入磁碟。
        for handle, _ in self.open.values():
            handle.close()


def check_markets(markets, mapper):
    # 在建立輸出前拒絕混用不同交易所的批次。
    foreign = set(markets).difference(mapper.types)
    if foreign:
        raise ValueError(f'{mapper.family.capitalize()} only: unexpected markets {sorted(foreign)}')


def in_window(timestamp, start, end):
    # 採用左閉右開的事件時間窗，窗前行情仍參與狀態重建。
    return (start is None or start <= timestamp) and (end is None or timestamp < end)


def replay_run(root, manifest, mapper, writer, options, report):
    # 逐筆讀取提交順序，先更新狀態，再以事件時間篩選待寫列。
    recover, start, end = options
    warnings = report['recovery_warnings']
    count = 0
    mapper.reset()
    report['inputs'].append({'run': str(root), 'manifest_status': manifest['status'],
                             'health': manifest.get('health_at_stop')})
    for record in read_records(root, recover, warnings):
        market, symbol = record['exchange'], record['symbol']
        if market:
            check_markets([market], mapper)
        local = record['local_timestamp']
        if symbol and market and in_window(local, start, end):
            writer.ensure(market, symbol, utc(local))
        for target, row in mapper.consume(record):
            if in_window(row['timestamp'], start, end):
                writer.write(target, row)
        count += 1
        report['raw_records'] += 1
        if count % 10000 == 0:
            mapper.db.commit()
        if count % 100000 == 0:
            LOG.info('event=normalization_progress raw_records=%s csv_rows=%s', count, sum(writer.counts.values()))
    check_count(root, manifest, count, recover, warnings)
    LOG.info('event=normalization_run_complete run=%s raw_records=%s', root, count)


def replay(mapper_class, source, output, recover=False, start=None, end=None):
    # 限定交易所並依批次起始時間重播，各批次重新等待該市場的快照。
    warnings = []
    runs = sorted(read_runs(source, recover, warnings), key=lambda item: item[1]['started_us'])
    previous_end = None
    for _, manifest in runs:
        markets = manifest.get('config', {}).get('markets', [manifest['exchange']] if 'exchange' in manifest else [])
        check_markets(markets, mapper_class)
        if previous_end is not None and manifest['started_us'] < previous_end:
            raise ValueError('overlapping runs require explicit replica merging; unsupported')
        previous_end = manifest.get('ended_us', manifest['started_us'])
    metadata = reference_metadata(mapper_class.family)
    report = dict(status='running', family=mapper_class.family, mapper_reference=metadata,
                  source_limitations=mapper_class.limitations, inputs=[], raw_records=0,
                  note='Replay completion and source coverage are assessed separately. See inputs and source_limitations.',
                  start_us=start, end_us=end, recovery_warnings=warnings)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    report_path = output / 'normalization_report.json'
    atomic_json(report_path, report)
    db = sqlite3.connect(output / '.dedup.sqlite')
    writer = Output(output, mapper_class.types)
    mapper = mapper_class(db)
    LOG.info('event=normalization_started input=%s output=%s runs=%s', source, output, len(runs))
    try:
        for root, manifest in runs:
            replay_run(root, manifest, mapper, writer, (recover, start, end), report)
        report['status'] = 'completed_with_warnings' if warnings else 'complete'
    except BaseException as exc:
        report.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        writer.close()
        db.close()
        (output / '.dedup.sqlite').unlink(missing_ok=True)
        report.update(rows=dict(writer.counts), timestamp_ranges=writer.ranges, mapper_issues=dict(mapper.issues))
        source_issues = any((i['health'] or {}).get('data_quality') != 'healthy' for i in report['inputs'])
        mapping_issues = set(mapper.issues).difference(('duplicate_trades', 'off_book_trades'))
        review = warnings or source_issues or mapping_issues or report['source_limitations'] or report['status'] == 'failed'
        report['data_quality'] = 'review_required' if review else 'no_detected_issue'
        atomic_json(report_path, report)
        LOG.info('event=normalization_finished status=%s rows=%s', report['status'], sum(writer.counts.values()))
    return report


def cli(mapper_class):
    # 各家執行入口共用參數與輸出報告格式。
    configure_logging()
    parser = argparse.ArgumentParser(description=f'{mapper_class.family} 原始 JSONL 離線轉換為 Tardis CSV')
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--recover', action='store_true')
    parser.add_argument('--start-us', type=int)
    parser.add_argument('--end-us', type=int)
    args = parser.parse_args()
    if args.start_us is not None and args.end_us is not None and args.start_us >= args.end_us:
        parser.error('invalid interval')
    report = replay(mapper_class, args.input, args.output, args.recover, args.start_us, args.end_us)
    print(json.dumps({k: report[k] for k in ('status', 'data_quality', 'rows')}))
