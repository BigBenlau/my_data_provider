"""第 02 階段共用的離線重播、版本追蹤、價位索引與 CSV 輸出。"""
from collections import OrderedDict
from contextlib import closing
import csv
import hashlib
import heapq
from datetime import datetime, timezone
from importlib import import_module
import json
import os
from pathlib import Path
import sqlite3
import sys
from common import SCHEMAS, LOG, STEP2_ROOT, atomic_json, dec, encode, utc
from common.raw_reader import read_runs, check_count
from common.segments import utc_day
from .replay_input import RunReader
from .replay_report import ReplayJournal

COMMIT = 'b75d1c5788c1724dbc6b73be0e1db2a158f93f57'
REFERENCE_MAPPER = {'binance': 'binance', 'bybit': 'bybit', 'okx': 'okex', 'coinbase': 'coinbase', 'upbit': 'upbit'}
DEFAULT_BATCH_SIZE = 4096


def reference_metadata(family):
    # 記下實際使用的原碼版本與本地檔案雜湊，離線重播不需連線。
    root = Path(__file__).parent
    paths = [f'src/mappers/{REFERENCE_MAPPER[family]}.ts', 'src/mappers/mapper.ts',
             'src/computable/booksnapshot.ts', 'src/orderbook.ts']
    if family in ('upbit', 'coinbase'):
        paths.append('src/handy.ts')
    sources = {Path(p).name: {'url': f'https://github.com/tardis-dev/tardis-node/blob/{COMMIT}/{p}',
                              'sha256': digest(root / 'reference' / Path(p).name)} for p in paths}
    local = [root / '02_normalize_tardis.py', root / 'normalization.py', root / 'replay_cli.py',
             root / 'replay_input.py', root / 'replay_report.py', root / 'mappers' / f'{family}.py',
             *(STEP2_ROOT / 'common').glob('*.py')]
    if family != 'binance':
        local.append(root / 'market_mapping.py')
    return {'commit': COMMIT, 'reviewed_on': '2026-11-26', 'sources': sources,
            'comparison_notes': str((root / 'reference' / 'README.md').relative_to(STEP2_ROOT)),
            'policy': 'tardis_mapping_with_strict_local_book_checks',
            'implementation_sha256': {str(p.relative_to(STEP2_ROOT)): digest(p) for p in sorted(local)}}


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
    def __init__(self, root, types, journal):
        # 限制同時開啟的檔案數，每家只建立其市場支援的表格。
        self.root = root
        self.types = types
        self.open = OrderedDict()
        self.groups = OrderedDict()
        self.pending = {}
        self.total = 0
        self.journal = journal

    def ensure(self, market, symbol, day):
        # 先建立當日表頭，讓有採集來源但未發生清算的日期仍有清算檔案。
        key = market, symbol, day
        if key in self.groups:
            self.groups.move_to_end(key)
            return
        for target in self.types[market]:
            rel = f'{market}/{target}/{day}_{symbol}.csv'
            path = self.root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                handle = path.open('x', newline='', encoding='utf-8')
            except FileExistsError:
                continue
            with handle:
                csv.writer(handle).writerow(SCHEMAS[target])
        self.groups[key] = None
        if len(self.groups) > 128:
            self.groups.popitem(last=False)

    def write(self, target, row, day):
        # 依事件時間分檔，保留十進位精度，並分別記下最早及最晚時間。
        market, symbol = row['exchange'], row['symbol']
        timestamp = row['timestamp']
        self.ensure(market, symbol, day)
        rel = f'{market}/{target}/{day}_{symbol}.csv'
        if rel not in self.open:
            if len(self.open) >= 40:
                self.open.popitem(last=False)[1][0].close()
            handle = (self.root / rel).open('a', newline='', encoding='utf-8')
            self.open[rel] = handle, csv.DictWriter(handle, fieldnames=SCHEMAS[target])
        self.open.move_to_end(rel)
        self.open[rel][1].writerow({k: encode(row.get(k)) for k in SCHEMAS[target]})
        count, low, high = self.pending.get(rel, (0, timestamp, timestamp))
        self.pending[rel] = count + 1, min(low, timestamp), max(high, timestamp)
        self.total += 1
        if len(self.pending) >= 128:
            self.flush_counts()

    def flush_counts(self):
        # 將已用完的逐檔統計寫到磁碟；活躍簿及成交去重由 mapper 獨立保留。
        self.journal.save_counts(self.pending)
        self.pending.clear()

    def close(self):
        # 將所有剩餘輸出緩衝寫入磁碟。
        error = None
        while self.open:
            handle, _ = self.open.popitem()[1]
            try:
                handle.close()
            except OSError as exc:
                error = exc
        self.flush_counts()
        if error:
            raise error


def check_markets(markets, mapper):
    # 在建立輸出前拒絕混用不同交易所的批次。
    foreign = set(markets).difference(mapper.types)
    if foreign:
        raise ValueError(f'{mapper.family.capitalize()} only: unexpected markets {sorted(foreign)}')


def in_window(timestamp, start, end):
    # 採用左閉右開的事件時間窗，窗前行情仍參與狀態重建。
    return (start is None or start <= timestamp) and (end is None or timestamp < end)


def replay_run(root, manifest, mapper, writer, options, report):
    # 每批只提交去重及報告索引，保留兩批邊界上的有效行情狀態。
    recover, start, end, batch_size = options
    journal = writer.journal
    journal.started(root, manifest)
    mapper.reset()
    reader = RunReader(root, manifest, mapper.db, journal, mapper.reset, recover, batch_size)
    mapper.on_invalid = reader.invalid_payload
    try:
        count = consume_run(reader, mapper, writer, start, end, batch_size, report)
    finally:
        mapper.on_invalid = None
    check_count(root, manifest, count, True, journal)
    mapper.reset()
    writer.flush_counts()
    mapper.db.commit()
    journal.db.commit()
    LOG.info('event=normalization_run_complete run=%s raw_records=%s', root, count)


def consume_run(reader, mapper, writer, start, end, batch_size, report):
    # 逐筆消費，每批清空統計暫存；產生器關閉時釋放檔案與 SQLite 讀取游標。
    count = 0
    with closing(reader.records()) as records:
        for record in records:
            consume_record(record, mapper, writer, reader, start, end)
            count += 1
            report['raw_records'] += 1
            if count % batch_size == 0:
                writer.flush_counts()
                mapper.db.commit()
                writer.journal.db.commit()
            if count % 100000 == 0:
                LOG.info('event=normalization_progress run=%s raw_records=%s csv_rows=%s', reader.root, count, writer.total)
    return count


def consume_record(record, mapper, writer, reader, start, end):
    # 跨市場的批次仍是輸入配置錯誤；行情缺欄逐筆記錄，輸出錯誤則向上回報。
    market, symbol = record['exchange'], record['symbol']
    if market and market not in mapper.types:
        check_markets([market], mapper)
    local = record['local_timestamp']
    if symbol and market and in_window(local, start, end):
        try:
            local_day = utc(local)
        except (OverflowError, ValueError, OSError) as exc:
            reader.issue('invalid local_timestamp', **reader.position, detail=str(exc))
            return
        writer.ensure(market, symbol, local_day)
    try:
        rows = mapper.consume(record)
    except (KeyError, TypeError, ValueError, IndexError, AttributeError, OverflowError) as exc:
        reader.issue('invalid mapper input', **reader.position, detail=str(exc))
        return
    for target, row in rows:
        if in_window(row['timestamp'], start, end):
            try:
                event_day = utc(row['timestamp'])
            except (OverflowError, ValueError, OSError) as exc:
                reader.issue('invalid event timestamp', **reader.position, detail=str(exc))
                continue
            writer.write(target, row, event_day)


def time_window(start, end, day):
    # 省略兩端表示全量，單側限制及 UTC 日期維持原有事件時間語義。
    if day is not None:
        if start is not None or end is not None:
            raise ValueError('choose --date or an explicit time window')
        midnight = datetime.strptime(utc_day(day), '%Y-%m-%d').replace(tzinfo=timezone.utc)
        start = int(midnight.timestamp()) * 1_000_000
        end = start + 86_400_000_000
    if start is not None and end is not None and start >= end:
        raise ValueError('invalid interval')
    return start, end


def invalid_payload(mapper, record, exc, item=None):
    # mapper 可單獨使用；正式重播另將每個損壞成員的實際錯誤送進來源日誌。
    mapper.issues['invalid_payload'] += 1
    handler = getattr(mapper, 'on_invalid', None)
    if handler is not None:
        handler(record, exc, item)


def checked_runs(source, mapper_class, recover, journal, day):
    # 建立輸出前拒絕錯誤交易所，也保留採集鎖和即時封段檢查。
    for candidate in run_roots(Path(source)):
        root, manifest = next(read_runs(candidate, recover, journal, day))
        markets = manifest.get('config', {}).get('markets', [manifest['exchange']] if 'exchange' in manifest else [])
        check_markets(markets, mapper_class)
        yield root, manifest


def run_roots(source):
    # 根目錄也可直接是一個 run；多批次的路徑逐一加入磁碟計畫。
    if (source / 'manifest.json').is_file():
        yield source
        return
    found = False
    if source.is_dir():
        with os.scandir(source) as entries:
            for entry in entries:
                root = Path(entry.path)
                if entry.is_dir() and (root / 'manifest.json').is_file():
                    found = True
                    yield root
    if not found:
        raise ValueError(f'no collector runs found: {source}')


def replay(mapper_class, source, output, recover=False, start=None, end=None, day=None, *, batch_size=DEFAULT_BATCH_SIZE, result='full'):
    # 使用磁碟暫存索引；只有需要完整回傳物件的 Python 呼叫端載入最終 JSON。
    start, end = time_window(start, end, day)
    if batch_size < 1:
        raise ValueError('batch size must be positive')
    with closing(sqlite3.connect('')) as scratch:
        journal = ReplayJournal(scratch)
        journal.plan(checked_runs(source, mapper_class, recover, journal, day))
        output = Path(output)
        report = dict(status='running', family=mapper_class.family, mapper_reference=reference_metadata(mapper_class.family),
                      source_limitations=mapper_class.limitations, raw_records=0, utc_date=day, start_us=start, end_us=end,
                      note='Replay completion and source coverage are assessed separately. See inputs and source_limitations.')
        output.mkdir(parents=True, exist_ok=False)
        path = output / 'normalization_report.json'
        atomic_json(path, dict(report, inputs=[], recovery_warnings=[]))
        LOG.info('event=normalization_started exchange=%s pid=%s input=%s output=%s', mapper_class.family, os.getpid(), source, output)
        index = output / '.dedup.sqlite'
        try:
            with closing(sqlite3.connect(index)) as db:
                db.execute('PRAGMA temp_store=FILE')
                writer = Output(output, mapper_class.types, journal)
                process_runs(mapper_class(db), writer, (recover, start, end, batch_size), report, path)
        finally:
            index.unlink(missing_ok=True)
        fields = {key: report[key] for key in ('status', 'data_quality')}
        if result == 'console':
            journal.dump(sys.stdout, fields, summary=True)
        if result == 'full':
            with path.open(encoding='utf-8') as stream:
                return json.load(stream)
        return dict(fields, raw_records=report['raw_records'], csv_rows=writer.total, output=str(output))


def process_runs(mapper, writer, options, report, path):
    # 即使正常化失敗也發布已處理範圍，所有可恢復的缺失都進入品質報告。
    journal = writer.journal
    try:
        try:
            for root, manifest in journal.runs():
                replay_run(root, manifest, mapper, writer, options, report)
        finally:
            writer.close()
        report['status'] = 'completed_with_warnings' if journal.warning_count else 'complete'
    except BaseException as exc:
        report.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        report.update(mapper_issues=dict(mapper.issues), warning_count=journal.warning_count)
        mapping_issues = set(mapper.issues).difference(('duplicate_trades', 'off_book_trades'))
        review = journal.warning_count or journal.source_issues or mapping_issues or report['source_limitations'] or report['status'] == 'failed'
        report['data_quality'] = 'review_required' if review else 'no_detected_issue'
        journal.publish(path, report)
        LOG.info('event=normalization_finished exchange=%s status=%s rows=%s', mapper.family, report['status'], writer.total)


def mapper_for(family):
    # 由交易所名稱載入對應的離線 mapper。
    return import_module(f'.mappers.{family}', __package__).Mapper


def cli():
    # 保留既有入口，單家及多家共用參數與狀態規則。
    from .replay_cli import main
    main()
