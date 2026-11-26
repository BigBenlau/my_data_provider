"""依提交序讀取原始分段，將可定位的輸入缺失交給重播報告。"""
from contextlib import closing, ExitStack
import gzip
import heapq
import json
from pathlib import Path
import sqlite3
import zlib

from common.segments import disk_names, first_seq, open_sealed


def catalog_rows(path, table, columns, warn):
    # 熱盤目錄與冷盤清單都只讀取名稱和封段範圍。
    if path.is_file():
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                    yield from db.execute(f'SELECT {columns} FROM {table}')
        except sqlite3.DatabaseError as exc:
            warn('unreadable input catalog', path=str(path), detail=str(exc))


def segment_plan(root, manifest, db, warn):
    # 用暫存磁碟索引合併每日目錄及兩顆磁碟的封段名稱，按原始序號排序。
    db.execute('DROP TABLE IF EXISTS replay_segments')
    db.execute('CREATE TEMP TABLE replay_segments(name TEXT PRIMARY KEY, first_seq INTEGER, last_seq INTEGER)')
    merged = manifest.get('schema_version') == 3
    sealed = manifest.get('sealed_through_seq')
    if sealed is None:
        for name in disk_names(root, '*.jsonl*'):
            if not name.endswith(('.jsonl', '.jsonl.gz', '.jsonl.gz.part', '.jsonl.part')):
                continue
            if merged:
                name = name.removesuffix('.gz')
            try:
                first = first_seq(name)
            except ValueError:
                first = 0
            db.execute('INSERT OR IGNORE INTO replay_segments VALUES (?,?,NULL)', (name, first))
    if merged:
        catalogs = [('_state/merge.sqlite', 'segments', 'name,first_seq,last_seq')]
        if sealed is None:
            catalogs.insert(0, ('_state/cold_catalog.sqlite', 'files', 'name,first,last'))
        for path, table, fields in catalogs:
            entries = catalog_rows(root / path, table, fields, warn)
            db.executemany('INSERT OR REPLACE INTO replay_segments VALUES (?,?,?)',
                           (row for row in entries if sealed is None or row[2] <= sealed))
    db.commit()
    return db.execute('SELECT name,first_seq,last_seq FROM replay_segments ORDER BY first_seq,name')


def open_input(root, name, merged):
    # 已封存的第三版沿用有雜湊和磁碟身分檢查的開檔流程。
    if merged and not name.endswith('.part'):
        try:
            return open_sealed(root, name)
        except sqlite3.DatabaseError as exc:
            raise OSError(f'cannot verify source catalog: {exc}') from exc
    path = root / 'raw' / name
    return gzip.open(path, 'rb') if name.endswith(('.gz', '.gz.part')) else path.open('rb')


def parse_record(line, require_newline):
    # 逐行解碼讓錯誤的 UTF-8 或 JSON 不會吞掉後面的完整紀錄。
    if require_newline and not line.endswith(b'\n'):
        raise ValueError('incomplete JSONL line')
    record = json.loads(line)
    if not isinstance(record, dict) or type(record.get('seq')) is not int or not 0 < record['seq'] <= 2**63-1:
        raise ValueError('missing or invalid seq')
    return record


def check_envelope(record):
    # 空市場的控制事件合法；有市場的行情必須具備對應的原文或事件名稱。
    required = ['exchange', 'symbol', 'kind', 'channel', 'local_timestamp']
    if record.get('exchange') and record.get('symbol'):
        if record.get('kind') in ('ws', 'rest', 'message', 'data'):
            required.append('raw')
        if record.get('kind') == 'event':
            required.append('event')
        if record['exchange'] in ('binance', 'binance-futures') and str(record.get('channel')).startswith('depth'):
            required.append('session')
    missing = [key for key in required if key not in record or record[key] is None and key not in ('symbol', 'exchange')]
    if missing:
        raise ValueError('missing fields: ' + ', '.join(missing))
    if type(record['local_timestamp']) is not int:
        raise ValueError('invalid local_timestamp')
    if any(record[key] is not None and not isinstance(record[key], str) for key in ('exchange', 'symbol', 'kind', 'channel')):
        raise ValueError('invalid exchange, symbol, kind or channel')


class RunReader:
    def __init__(self, root, manifest, db, warnings, reset, recover, batch_size):
        # 每個 run 各自追蹤提交游標，只有發現缺失才使行情狀態失效。
        self.root, self.manifest, self.db = Path(root), manifest, db
        self.warnings, self.reset = warnings, reset
        self.recover, self.batch_size = recover, batch_size
        self.merged = manifest.get('schema_version') == 3
        self.expected = 1
        self.at = None
        self.committed = None
        if recover and self.merged:
            rows = catalog_rows(self.root / '_state/merge.sqlite', 'output', 'written', self.issue)
            with closing(rows):
                self.committed = next((row[0] for row in rows), None)
            if self.committed is None and manifest['status'] != 'running':
                self.committed = manifest.get('raw_records')

    @property
    def position(self):
        # 只有發出警告時才組合完整路徑。
        name, line, seq = self.at
        return dict(path=str(self.root / 'raw' / name), line=line, seq=seq)

    def issue(self, reason, name=None, *, invalidate=True, **fields):
        # 記錄來源、行號及已知序號；未知缺口不能沿用舊簿或 ticker。
        warning = dict(run=str(self.root), reason=reason, **fields)
        if name is not None:
            warning['path'] = str(self.root / 'raw' / name)
        self.warnings.append(warning)
        if invalidate:
            self.reset()

    def gap(self, end, name=None, **fields):
        # 一次標示整個缺少的區間，避免為大量連續缺行建立逐筆物件。
        if end >= self.expected:
            self.issue('sequence gap', name, first_seq=self.expected, last_seq=end, **fields)
            self.expected = end + 1

    def invalid_payload(self, record, exc, item):
        # 原生訊息的缺欄或解析錯誤也保留行號與批內成員，而非只有總次數。
        fields = dict(self.position, exchange=record['exchange'], symbol=record['symbol'],
                      channel=record['channel'], detail=str(exc))
        if item is not None:
            fields['item_index'] = item
        stateful = record['exchange'] in ('binance', 'binance-futures') and (
            record['channel'].startswith('depth') or record['channel'] in ('ticker', 'markPrice@1s', 'openInterest'))
        self.issue('invalid payload', invalidate=stateful, **fields)

    def lines(self, name, last=None, legacy=False):
        # 開檔、解壓或單行失敗分開處理，仍可前往下一行或下一段。
        partial = name.endswith('.part')
        if partial and not self.recover:
            self.issue('unsealed segment skipped', name, expected_seq=self.expected)
            return
        if partial and self.merged and self.committed is None:
            self.issue('missing committed boundary for partial segment', name, expected_seq=self.expected)
            return
        number = previous = 0
        try:
            with open_input(self.root, name, self.merged) as source:
                for number, line in enumerate(source, 1):
                    if partial and self.committed is not None and self.expected > self.committed:
                        break
                    try:
                        record = parse_record(line, not legacy)
                    except (ValueError, RecursionError) as exc:
                        self.issue('invalid JSONL', name, invalidate=not legacy, line=number, detail=str(exc),
                                   expected_seq=self.expected)
                        continue
                    seq = record['seq']
                    if legacy and seq <= previous:
                        self.issue('non-increasing legacy sequence', name, invalidate=False,
                                   line=number, seq=seq, previous_seq=previous)
                        continue
                    previous = seq
                    if partial and self.committed is not None and seq > self.committed:
                        break
                    if last is not None and seq > last:
                        self.issue('outside sealed range', name, invalidate=False, line=number, seq=seq, last_seq=last)
                        continue
                    yield seq, name, number, record
        except (OSError, EOFError, zlib.error) as exc:
            self.issue('unreadable segment', name, invalidate=not legacy, after_line=number,
                       expected_seq=self.expected, detail=str(exc))

    def segment_records(self, segments):
        # 在封段邊界也核對序號，因此整個尾段缺失仍會列出範圍。
        for name, first, last in segments:
            if name.endswith('.part') and self.committed is not None and first > self.committed:
                continue
            if first:
                self.gap(first - 1, name)
            yield from self.lines(name, last)
            if last is not None:
                self.gap(last, name)

    def legacy_records(self, segments):
        # 舊版各檔序號可能交錯；少量檔案串流合併，大量檔案改用磁碟排序。
        count = self.db.execute('SELECT count(*) FROM replay_segments').fetchone()[0]
        if count <= 32:
            with ExitStack() as stack:
                sources = [stack.enter_context(closing(self.lines(name, legacy=True))) for name, _, _ in segments]
                yield from heapq.merge(*sources, key=lambda item: item[0])
            return
        self.db.execute('CREATE TEMP TABLE legacy_sort(seq INTEGER, name TEXT, line INTEGER, payload TEXT, '
                        'PRIMARY KEY(seq,name,line)) WITHOUT ROWID')
        try:
            staged = 0
            for name, _, _ in segments:
                for seq, _, number, record in self.lines(name, legacy=True):
                    self.db.execute('INSERT INTO legacy_sort VALUES (?,?,?,?)', (seq, name, number, json.dumps(record)))
                    staged += 1
                    if staged % self.batch_size == 0:
                        self.db.commit()
            self.db.commit()
            for seq, name, number, payload in self.db.execute('SELECT * FROM legacy_sort ORDER BY seq,name,line'):
                yield seq, name, number, json.loads(payload)
        finally:
            self.db.execute('DROP TABLE legacy_sort')

    def records(self):
        # 全程只留目前一筆及必要的合併游標；缺口兩側的成交保留同一去重索引。
        segments = segment_plan(self.root, self.manifest, self.db, self.issue)
        legacy = not self.merged and self.db.execute(
            "SELECT 1 FROM replay_segments WHERE name LIKE '%.jsonl' LIMIT 1").fetchone()
        if legacy and self.db.execute("SELECT 1 FROM replay_segments WHERE name NOT LIKE '%.jsonl' LIMIT 1").fetchone():
            raise ValueError('mixed legacy and v2 segments')
        source = self.legacy_records(segments) if legacy else self.segment_records(segments)
        with closing(segments), closing(source):
            for seq, name, number, record in source:
                if seq < self.expected:
                    self.issue('duplicate/reversed sequence', name, invalidate=False, line=number, seq=seq,
                               expected_seq=self.expected)
                    continue
                self.gap(seq - 1, name, line=number)
                self.expected = seq + 1
                try:
                    check_envelope(record)
                except ValueError as exc:
                    self.issue('invalid envelope', name, line=number, seq=seq, detail=str(exc))
                    continue
                if legacy:
                    record['legacy_schema'] = True
                self.at = name, number, seq
                yield record
        end = self.manifest.get('sealed_through_seq')
        if end is None:
            end = self.committed if self.manifest['status'] == 'running' else self.manifest.get('raw_records')
        if end is not None:
            self.gap(end, '.')
