"""已提交資料的無損封存；歷史去重證據不設刪除期限。"""
from common.segments import checksum
import gzip
import hashlib
import os
import sqlite3
from pathlib import Path
import time


def sync_directory(path):
    # 檔名發布與移除也需持久化，避免僅同步檔案內容留下崩潰窗口。
    descriptor = os.open(path, os.O_RDONLY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)


def compress_segment(path):
    # 驗證解壓雜湊並持久發布後才移除原檔；中途失敗保留原始資料。
    if not path.exists():
        if not path.with_suffix(path.suffix + '.gz').exists(): raise FileNotFoundError(path)
        return
    temporary = path.with_suffix(path.suffix + '.gz.tmp'); target = path.with_suffix(path.suffix + '.gz')
    original = hashlib.sha256()
    with path.open('rb') as source, temporary.open('wb') as destination:
        with gzip.GzipFile(filename='', mode='wb', fileobj=destination, compresslevel=1, mtime=0) as compressed:
            for block in iter(lambda: source.read(1024 * 1024), b''):
                original.update(block); compressed.write(block)
        destination.flush(); os.fsync(destination.fileno())
    verified = hashlib.sha256()
    with gzip.open(temporary, 'rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''): verified.update(block)
    if original.digest() != verified.digest(): raise OSError('compressed segment checksum mismatch')
    os.replace(temporary, target); sync_directory(path.parent)
    path.unlink(); sync_directory(path.parent)


def compress_ready(root, limit=8):
    # 壓縮只處理已封段的原文；獨立連線不使用 supervisor 的收訊鎖或 CSV 游標。
    db = sqlite3.connect(Path(root) / '_state' / 'merge.sqlite', timeout=30)
    try:
        db.execute('PRAGMA synchronous=FULL')
        names = db.execute('SELECT name FROM segments WHERE compressed=0 ORDER BY first_seq LIMIT ?', (limit,)).fetchall()
        for (name,) in names:
            compress_segment(Path(root) / 'raw' / name)
            zipped = Path(root)/'raw'/(name+'.gz')
            digest = checksum(zipped)
            with db:
                db.execute('UPDATE segments SET compressed=1 WHERE name=?', (name,))
                db.execute('UPDATE sealed_at SET bytes=?,digest=? WHERE name=?',(zipped.stat().st_size,digest,name))
        return len(names)
    finally:
        db.close()


class IndexArchive:
    """熱索引有界搬移至歷史索引；遲到副本仍可查證，不用 TTL 忽略事件。"""
    tables = ('seen', 'edges', 'variants')

    def __init__(self, db, root, evidence):
        # 複製與刪除分成兩次耐久交易，跨資料庫崩潰最多留下重複索引。
        self.db = db
        self.evidence = evidence
        self.root = Path(root)
        db.execute('ATTACH DATABASE ? AS archive', (str(Path(root) / '_state' / 'index_archive.sqlite'),))
        db.execute('PRAGMA archive.journal_mode=WAL'); db.execute('PRAGMA archive.synchronous=FULL')
        db.execute('PRAGMA archive.cache_size=-8192')
        with db:
            for table in self.tables:
                db.execute(f'CREATE TABLE IF NOT EXISTS archive.{table} AS SELECT * FROM main.{table} WHERE 0')
                keys = 'seq'
                db.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS archive.{table}_key ON {table}({keys})')
            db.execute('CREATE INDEX IF NOT EXISTS archive.event_lookup ON seen(feed,ident,digest)')
            db.execute('CREATE INDEX IF NOT EXISTS archive.edge_lookup ON edges(feed,u)')
        db.execute('CREATE TEMP VIEW IF NOT EXISTS all_edges AS SELECT * FROM main.edges UNION SELECT * FROM archive.edges')
        self.has_seen = bool(db.execute('SELECT 1 FROM archive.seen LIMIT 1').fetchone())

    def holds(self,cutoff):
        # 只有實際到期的明細才讀取各副本的交付區間。
        if not self.db.execute('SELECT 1 FROM observations WHERE observed<? LIMIT 1',(cutoff,)).fetchone():
            return
        from .pending_ranges import collect
        ranges = collect(self.root/'_spool',cutoff)
        if not ranges and not self.db.execute('SELECT 1 FROM pending_evidence LIMIT 1').fetchone():
            return
        with self.db:
            self.db.execute('DELETE FROM pending_evidence')
            self.db.executemany('INSERT INTO pending_evidence SELECT id,?,? FROM feed_names WHERE name=?',
                                [(first-5,last+5,name) for name,first,last in ranges])

    def move(self, seconds, limit=2000):
        # 每輪只處理有限索引頁；最近觀察過或尚未落盤的事件保留於熱區。
        cutoff = time.time() - seconds
        evidence_cutoff = time.time()-86400
        self.holds(evidence_cutoff)
        self.db.execute('CREATE TEMP TABLE IF NOT EXISTS moving(seq INTEGER PRIMARY KEY)')
        written = self.db.execute('SELECT written FROM output').fetchone()[0]
        with self.db:
            self.db.execute('DELETE FROM moving')
            # 鍵集分頁跨過暫時不可搬移的資料，避免熱區首批永遠擋住後方舊資料。
            floor = getattr(self, 'scanned', 0)
            if not floor:
                self.scan_end = written
            upper = self.db.execute('SELECT max(seq) FROM (SELECT seq FROM seen WHERE seq>? AND seq<=? '
                                    'ORDER BY seq LIMIT ?)', (floor, self.scan_end, limit)).fetchone()[0] or floor
            self.db.execute('INSERT INTO moving SELECT seq FROM seen WHERE seq>? AND seq<=? AND received<?',
                            (floor, upper, cutoff * 1e6))
            self.scanned = upper if floor < upper < self.scan_end else 0
        count = self.db.execute('SELECT count(*) FROM moving').fetchone()[0]
        if not count:
            with self.db:
                self.evidence.summarize(evidence_cutoff, limit)
            return 0
        # 兩次耐久交易保留原有崩潰邊界；採集合併持鎖期間每表各做一次集合操作。
        with self.db:
            for table in self.tables:
                self.db.execute(f'INSERT OR IGNORE INTO archive.{table} SELECT * FROM main.{table} WHERE seq IN moving')
        self.has_seen = True
        with self.db:
            self.evidence.summarize(evidence_cutoff, limit)
            # 事故範圍保留在熱盤，供後續保護判斷與查詢。
            self.db.execute('DELETE FROM moving WHERE seq IN (SELECT seq FROM observations)')
            for table in self.tables:
                self.db.execute(f'DELETE FROM main.{table} WHERE seq IN moving')
        return count
