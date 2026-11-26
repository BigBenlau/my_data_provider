"""從已封存 JSONL 按需增量匯出原生 channel，採集與匯出各自持有游標。"""
from collections import defaultdict
import csv
import fcntl
import gzip
import io
import json
import os
from pathlib import Path

from common import LOG, utc
from common.segments import catalog_entries, segment_batches, utc_day, sealed_snapshot
from .delivery import database
from .raw import ChannelCSV, SCHEMA_VERSION, channel_row, header
from .retention import sync_directory

SCHEMA = """
CREATE TABLE IF NOT EXISTS cursor(source TEXT PRIMARY KEY,seq INTEGER,rows INTEGER);
CREATE TABLE IF NOT EXISTS files(name TEXT PRIMARY KEY,offset INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS daily_cursor(day TEXT PRIMARY KEY,seq INTEGER);
CREATE TABLE IF NOT EXISTS days(day TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pending(name TEXT PRIMARY KEY,offset INTEGER);
CREATE TABLE IF NOT EXISTS recent(name TEXT PRIMARY KEY);
"""


class Exporter:
    """每次匯出只修改使用者指定的專用輸出目錄。"""
    def __init__(self, root, output, day=None):
        # 以來源的封裝、建立時間及身分綁定輸出，可移動備份後接續同一批。
        self.root = Path(root).resolve()
        self.target = Path(output).resolve()
        if self.target == self.root or any(self.target.is_relative_to(self.root / d) for d in ('raw', '_state', '_spool')):
            raise ValueError('choose a dedicated export directory outside collector storage')
        manifest = json.loads((self.root / 'manifest.json').read_text())
        if manifest.get('schema_version') != 3 or manifest.get('channel_csv_schema_version', 1) != SCHEMA_VERSION:
            raise ValueError('export-csv requires supported supervisor data; use replay for older formats')
        self.day = utc_day(day) if day is not None else None
        self.run_id = manifest['run_id']
        self.started = manifest['started_us']
        self.config = manifest['config']
        self.source = json.dumps([self.run_id, self.started, manifest.get('identity'), self.config], sort_keys=True)
        self.limit = sealed_snapshot(self.root, self.day) if self.day and manifest['status'] == 'running' else None
        self.ended = manifest.get('ended_us') if manifest['status'] == 'stopped' else None
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
        self.ahead = dict(self.db.execute('SELECT day,seq FROM daily_cursor WHERE seq>?', (seq,)))
        if self.day:
            seq = max(seq, self.ahead.get(self.day, 0))
        initial = self.day or utc(self.started)
        if self.covers(initial) and not self.db.execute('SELECT 1 FROM days WHERE day=?', (initial,)).fetchone():
            self.write_batch([], seq, rows, {initial})
        seq, rows = self.export_segments(seq, rows)
        files = self.db.execute('SELECT count(*) FROM files').fetchone()[0]
        LOG.info('event=csv_exported day=%s seq=%s rows=%s files=%s output=%s', self.day, seq, rows, files, self.target)
        return dict(seq=seq, rows=rows, files=files)

    def covers(self, day):
        # 已知批次時段內的靜默日也可匯出只有表頭的每日檔案。
        if day == utc(self.started):
            return True
        if self.ended is None and self.last is not None:
            return False
        return day >= utc(self.started) and (self.ended is None or day <= utc(self.ended))

    def advance(self, seq, rows):
        # 全批次與逐日共用同一組輸出；只有全量掃描才可推進全域序號。
        if self.day:
            self.db.execute('INSERT OR REPLACE INTO daily_cursor VALUES(?,?)', (self.day, seq))
            self.db.execute('UPDATE cursor SET rows=?', (rows,))
        else:
            self.db.execute('UPDATE cursor SET seq=?,rows=?', (seq, rows))

    def export_segments(self, seq, rows):
        # 以目錄排除其他日期，歷史平面段則逐列篩選；提交序號前進仍會檢查全程連續。
        for name, first, last in catalog_entries(self.root, seq, self.last, limit=self.limit, day=self.day):
            if not first <= seq + 1 <= last:
                raise ValueError('archive sequence gap')
            directory = Path(name).parent.as_posix()
            if self.day and directory not in ('.', self.day):
                seq = last
                continue
            for batch in segment_batches(self.root, name, first, last, seq):
                seq = batch[-1]['seq']
                if self.day and directory == '.':
                    batch = [r for r in batch if utc(r['local_timestamp']) == self.day]
                elif not self.day and self.ahead:
                    # 逐日先匯出的序號已在正式 CSV 中，全量追趕只補其他日期與遲交列。
                    batch = [r for r in batch if r['seq'] > self.ahead.get(utc(r['local_timestamp']), 0)]
                if batch:
                    rows += self.write_batch(batch, seq, rows)
        if self.last is not None and seq != self.last:
            raise ValueError('archive cursor does not match stopped manifest')
        with self.db:
            self.advance(seq, rows)
            if not self.day:
                self.db.execute('DELETE FROM daily_cursor WHERE seq<=?', (seq,))
        return seq, rows

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
            self.db.execute('DELETE FROM recent')

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
            self.db.executemany('INSERT OR IGNORE INTO recent VALUES(?)', [(name,) for name, _ in written])
            self.advance(seq, rows)
            self.db.execute('DELETE FROM pending')
