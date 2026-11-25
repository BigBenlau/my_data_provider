"""從已封存 JSONL 按需增量匯出原生 channel，採集與匯出各自持有游標。"""
from collections import defaultdict
import csv
import fcntl
import gzip
import io
import json
import os
from pathlib import Path
import sqlite3

from common import LOG, utc
from .delivery import database
from .raw import ChannelCSV, SCHEMA_VERSION, channel_row, header
from .retention import sync_directory

SCHEMA = """
CREATE TABLE IF NOT EXISTS cursor(source TEXT PRIMARY KEY,seq INTEGER,rows INTEGER);
CREATE TABLE IF NOT EXISTS files(name TEXT PRIMARY KEY,offset INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS days(day TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pending(name TEXT PRIMARY KEY,offset INTEGER);
CREATE TABLE IF NOT EXISTS recent(name TEXT PRIMARY KEY);
"""


def archive_entries(root, cursor, last):
    # 唯讀備份可只保留 manifest 與 JSONL；以原始起始序號及停止筆數重建目錄。
    names = {p.name.removesuffix('.gz') for p in (root / 'raw').iterdir()
             if p.name.endswith(('.jsonl', '.jsonl.gz'))}
    starts = sorted((int(name.split('_')[0]), name) for name in names)
    if (not starts and last) or (starts and (starts[0][0] != 1 or starts[-1][0] > last)):
        raise ValueError('archive does not match stopped manifest')
    for index, (first, name) in enumerate(starts):
        end = starts[index + 1][0] - 1 if index + 1 < len(starts) else last
        if end < first:
            raise ValueError('invalid archived sequence range')
        if end > cursor:
            yield name, first, end


def catalog_entries(root, cursor, last):
    # 即時目錄固定已提交上限，跨段讀資料期間釋放讀取交易，避免阻礙 WAL。
    db = None
    try:
        try:
            uri = (root / '_state/merge.sqlite').as_uri() + '?mode=ro'
            db = sqlite3.connect(uri, uri=True)
            end = db.execute('SELECT coalesce(max(last_seq),0) FROM segments').fetchone()[0]
        except sqlite3.OperationalError:
            if last is None:
                raise
            # 唯讀掛載無法建立 SQLite 的 WAL 輔助檔；停止批次可直接核對封段。
            yield from archive_entries(root, cursor, last)
            return
        if cursor > end or (last is not None and last != end):
            raise ValueError('sealed catalog does not match source or export cursor')
        while cursor < end:
            row = db.execute('SELECT name,first_seq,last_seq FROM segments WHERE last_seq>? '
                             'AND last_seq<=? ORDER BY last_seq LIMIT 1', (cursor, end)).fetchone()
            if not row or not row[1] <= cursor + 1 <= row[2]:
                raise ValueError('sealed segment catalog gap')
            yield row
            cursor = row[2]
    finally:
        if db is not None:
            db.close()


def source_batches(root, cursor, last):
    # 只輸出與已提交游標相接的封段，停止批次另核對最終總數。
    for name, first, end in catalog_entries(root, cursor, last):
        if not first <= cursor + 1 <= end:
            raise ValueError('archive sequence gap')
        yield from segment_batches(root / 'raw' / name, first, end, cursor)
        cursor = end
    if last is not None and cursor != last:
        raise ValueError('archive cursor does not match stopped manifest')


def segment_batches(path, first, last, cursor):
    # 直接開檔以容許主管同時發布 gzip；已開啟的原檔即使改名仍可讀完。
    try:
        source = path.open('rb')
    except FileNotFoundError:
        source = gzip.open(str(path) + '.gz', 'rb')
    batch = []
    size = 0
    expected = first
    with source:
        for line in source:
            record = json.loads(line)
            if not line.endswith(b'\n') or record['seq'] != expected or expected > last:
                raise ValueError('sealed JSONL sequence or line mismatch')
            expected += 1
            if record['seq'] <= cursor:
                continue
            batch.append(record)
            size += len(line)
            if size >= 8 * 1024**2 or len(batch) == 8192:
                yield batch
                batch, size = [], 0
        if expected != last + 1:
            raise ValueError('sealed JSONL truncated')
        if batch:
            yield batch


class Exporter:
    """每次匯出只修改使用者指定的專用輸出目錄。"""
    def __init__(self, root, output):
        # 以來源的封裝、建立時間及身分綁定輸出，可移動備份後接續同一批。
        self.root = Path(root).resolve()
        self.target = Path(output).resolve()
        if self.target == self.root or any(self.target.is_relative_to(self.root / d) for d in ('raw', '_state', '_spool')):
            raise ValueError('choose a dedicated export directory outside collector storage')
        manifest = json.loads((self.root / 'manifest.json').read_text())
        if manifest.get('schema_version') != 3 or manifest.get('channel_csv_schema_version', 1) != SCHEMA_VERSION:
            raise ValueError('export-csv requires supported supervisor data; use replay for older formats')
        self.run_id = manifest['run_id']
        self.started = manifest['started_us']
        self.config = manifest['config']
        self.source = json.dumps([self.run_id, self.started, manifest.get('identity'), self.config], sort_keys=True)
        self.last = manifest['raw_records'] if manifest['status'] == 'stopped' else None

    def run(self):
        # 同一目的地僅允許一個寫入者；舊版 channels 或其他採集成果不可覆寫。
        self.target.mkdir(parents=True, exist_ok=True)
        with (self.target / '.export.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            path = self.target / 'export.sqlite'
            if not path.exists() and any(p.name != '.export.lock' for p in self.target.iterdir()):
                raise FileExistsError('choose a new export directory')
            self.db = database(path)
            try:
                return self.export()
            finally:
                self.db.close()

    def export(self):
        # 先撤回尚未提交的 gzip 成員，再從上一個耐久游標開始。
        self.db.executescript(SCHEMA)
        with self.db:
            old = self.db.execute('SELECT source FROM cursor').fetchone()
            if old and old[0] != self.source:
                raise ValueError('export directory belongs to another run')
            self.db.execute('INSERT OR IGNORE INTO cursor VALUES(?,0,0)', (self.source,))
        self.recover()
        seq, rows = self.db.execute('SELECT seq,rows FROM cursor').fetchone()
        if not self.db.execute('SELECT 1 FROM days LIMIT 1').fetchone():
            self.write_batch([], seq, rows, {utc(self.started)})
        for batch in source_batches(self.root, seq, self.last):
            seq = batch[-1]['seq']
            rows += self.write_batch(batch, seq, rows)
        files = self.db.execute('SELECT count(*) FROM files').fetchone()[0]
        LOG.info('event=csv_exported seq=%s rows=%s files=%s output=%s', seq, rows, files, self.target)
        return dict(seq=seq, rows=rows, files=files)

    def recover(self):
        # 回滾的是壓縮檔實際位元組偏移，只刪除本次意圖登記的新檔案。
        query = '''SELECT name,offset FROM pending UNION ALL
                   SELECT f.name,f.offset FROM recent r JOIN files f USING(name)
                   WHERE f.name NOT IN (SELECT name FROM pending)'''
        for name, offset in self.db.execute(query):
            path = self.target / name
            if offset is None:
                if path.exists():
                    path.unlink()
                    sync_directory(path.parent)
                continue
            size = path.stat().st_size
            if size < offset:
                raise OSError(f'committed CSV truncated: {name}')
            if size > offset:
                with path.open('r+b') as handle:
                    handle.truncate(offset)
                    os.fsync(handle.fileno())
        with self.db:
            self.db.execute('DELETE FROM pending')

    def arrange(self, batch, days):
        # 分批解析一次；只為首次見到的日期列舉所有來源，保留靜默來源表頭。
        groups = defaultdict(list)
        for record in batch:
            day = utc(record['local_timestamp'])
            days.add(day)
            row = channel_row(record, self.run_id)
            if row is not None:
                name = ChannelCSV.relative(record['exchange'], record['symbol'], day, record['channel'])
                groups[name + '.gz'].append(row)
        new_days = {d for d in days if not self.db.execute('SELECT 1 FROM days WHERE day=?', (d,)).fetchone()}
        for day in new_days:
            for market, settings in self.config['market_settings'].items():
                for symbol in settings['symbols']:
                    for channel in settings['channels']:
                        groups.setdefault(ChannelCSV.relative(market, symbol, day, channel) + '.gz', [])
        return groups, new_days

    def write_batch(self, batch, seq, rows, days=None):
        # 先提交受影響檔案的原始偏移，檔案與新增目錄同步後才推進游標。
        groups, new_days = self.arrange(batch, set() if days is None else days)
        offsets = {}
        for name in groups:
            old = self.db.execute('SELECT offset FROM files WHERE name=?', (name,)).fetchone()
            offsets[name] = old[0] if old else None
            if old is None and (self.target / name).exists():
                raise FileExistsError(self.target / name)
        with self.db:
            self.db.executemany('INSERT INTO pending VALUES(?,?)', offsets.items())
        written = [(name, self.append(name, offsets[name], values)) for name, values in groups.items()]
        # 只有新檔或其上層目錄涉及檔名發布；各級由內而外同步一次。
        parents = {p for name, old in offsets.items() if old is None for p in (self.target / name).parents
                   if p == self.target or self.target in p.parents}
        for directory in sorted(parents, key=lambda p: len(p.parts), reverse=True):
            sync_directory(directory)
        count = sum(map(len, groups.values()))
        self.commit(written, new_days, seq, rows + count)
        return count

    def append(self, name, offset, values):
        # 直接追加獨立 gzip 成員；跨日遲到只寫新增列，無須解壓整日歷史。
        path = self.target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        market, _, _, filename = Path(name).parts
        with path.open('xb' if offset is None else 'r+b') as file:
            if offset is not None and file.seek(0, os.SEEK_END) != offset:
                raise OSError(f'committed CSV offset mismatch: {name}')
            with gzip.GzipFile(filename='', mode='wb', fileobj=file, compresslevel=1, mtime=0) as compressed:
                with io.TextIOWrapper(compressed, encoding='utf8', newline='') as stream:
                    writer = csv.DictWriter(stream, header(market, filename.removesuffix('.csv.gz')), lineterminator='\n')
                    if offset is None:
                        writer.writeheader()
                    writer.writerows(values)
            file.flush()
            os.fsync(file.fileno())
            return file.tell()

    def commit(self, written, days, seq, rows):
        # 資料列與空來源表頭共用同一交易，崩潰重跑不重複發布。
        with self.db:
            self.db.executemany('INSERT OR REPLACE INTO files VALUES(?,?)', written)
            self.db.executemany('INSERT INTO days VALUES(?)', [(day,) for day in days])
            self.db.execute('DELETE FROM recent')
            self.db.executemany('INSERT INTO recent VALUES(?)', [(name,) for name, _ in written])
            self.db.execute('UPDATE cursor SET seq=?,rows=?', (seq, rows))
            self.db.execute('DELETE FROM pending')
