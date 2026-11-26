"""在獨立裝置複製並驗證已封段行情與永久索引。"""
from contextlib import closing
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time
import uuid
from common.segments import volume, checksum
from .leadership import publish
from .retention import sync_directory


class HistoryUnavailable(OSError):
    """需要確切歷史查重但冷盤暫時不可讀。"""


def initialize(root, family):
    # 綁定已由操作員標識的磁碟。缺盤時不建立任何冷盤目錄。
    root = Path(root)
    marker = root/'_state/cold.json'
    if marker.exists():
        return json.loads(marker.read_text())
    configured = os.environ.get('COLD_ROOT')
    if not configured:
        return None
    device = Path(configured).resolve()
    identity = (device/'.collector-volume-id').read_text().strip()
    if not identity or identity != os.environ.get('COLD_VOLUME_ID',identity):
        raise OSError('cold volume identity mismatch')
    binding = dict(volume_id=identity,root=str(device),directory=family+'/'+uuid.uuid4().hex)
    directory = volume(root,binding)
    directory.mkdir(parents=True)
    (directory/'raw').mkdir()
    sync_directory(directory.parent)
    publish(marker,binding)
    return binding


def copy_file(source, target):
    # 只發布已同步的暫存副本；中斷時來源與舊的已發布副本仍可讀。
    temporary = target.with_name(target.name+'.part')
    with source.open('rb') as src, temporary.open('wb') as dst:
        shutil.copyfileobj(src,dst,1024*1024)
        dst.flush()
        os.fsync(dst.fileno())
    os.replace(temporary,target)
    sync_directory(target.parent)


def validate_segment(path, first, last):
    # 搬移後逐行解壓驗證序號、封尾及筆數。
    expected = first
    with gzip.open(path,'rb') as handle:
        for line in handle:
            if not line.endswith(b'\n') or json.loads(line)['seq'] != expected:
                raise OSError(f'cold segment discontinuity {path} seq={expected}')
            expected += 1
    if expected != last+1:
        raise OSError(f'cold segment truncated {path} seq={expected}..{last}')


def relocate(root, binding, db, row):
    # 冷目錄先提交、熱盤位置目錄再提交，最後才可移除已校驗的熱副本。
    name,first,last,sealed_digest = row
    directory = volume(root,binding)
    source = root/'raw'/(name+'.gz')
    target = directory/'raw'/(name+'.gz')
    if not target.parent.exists():
        target.parent.mkdir()
        sync_directory(target.parent.parent)
    if not source.exists():
        entry = db.execute('SELECT digest FROM files WHERE name=?',(name,)).fetchone()
        if entry and checksum(target)==entry[0]==sealed_digest:
            return
        raise FileNotFoundError(source)
    digest = checksum(source)
    if digest != sealed_digest:
        raise OSError(f'hot checksum changed since sealing seq={first}..{last}')
    copy_file(source,target)
    if checksum(target) != digest:
        raise OSError(f'cold copy SHA-256 mismatch seq={first}..{last}')
    validate_segment(target,first,last)
    entry = (name,first,last,digest)
    with closing(sqlite3.connect(directory/'catalog.sqlite')) as remote, remote:
        remote.execute(FILES)
        remote.execute('INSERT OR REPLACE INTO files VALUES(?,?,?,?)',entry)
    sync_directory(directory)
    with db:
        db.execute('INSERT OR REPLACE INTO files VALUES(?,?,?,?)',entry)
    volume(root,binding)
    source.unlink()
    sync_directory(source.parent)


FILES = 'CREATE TABLE IF NOT EXISTS files(name TEXT PRIMARY KEY,first INTEGER,last INTEGER,digest TEXT)'


def archive_files(root, binding, age=86400):
    # 封段時間按持久化的封段紀錄計算，每輪只搬一段以限制 I/O。
    if not binding:
        return
    root = Path(root)
    directory = volume(root,binding)
    manifest = directory/'manifest.json'
    if not manifest.exists() or checksum(root/'manifest.json') != checksum(manifest):
        copy_file(root/'manifest.json',manifest)
    with closing(sqlite3.connect(root/'_state/cold_catalog.sqlite')) as catalog:
        catalog.execute(FILES)
        with closing(sqlite3.connect(root/'_state/merge.sqlite')) as db:
            row = db.execute('SELECT s.name,first_seq,last_seq,a.digest FROM segments s JOIN sealed_at a USING(name) '
                             'WHERE compressed=1 AND a.closed<=? AND a.moved=0 ORDER BY first_seq LIMIT 1',
                             (time.time()-age,)).fetchone()
            if row:
                relocate(root,binding,catalog,row)
                with db:
                    db.execute('UPDATE sealed_at SET moved=1 WHERE name=?',(row[0],))


class ColdIndex:
    def __init__(self, db, root, family):
        # 歷史資料庫不附加至行情交易，冷盤故障不能回滾其他來源的熱盤提交。
        self.db = db
        self.root = Path(root)
        self.family = family
        self.binding = None
        self.error = None
        self.configure()
        self.remote = None
        db.executescript("""CREATE TABLE IF NOT EXISTS cold_keys(feed,short,PRIMARY KEY(feed,short)) WITHOUT ROWID;
          CREATE TABLE IF NOT EXISTS cold_state(name TEXT PRIMARY KEY);""")
        self.populated = bool(db.execute('SELECT 1 FROM cold_keys LIMIT 1').fetchone())

    def configure(self):
        # 首次部署時冷盤尚未上線也可先收訊；重新掛載成功才允許搬移。
        if not self.binding:
            try:
                self.binding = initialize(self.root,self.family)
                self.error = None
            except (OSError,ValueError) as exc:
                self.error = str(exc)

    def connect(self, create=False):
        # 每次使用前重新驗證裝置身分；已發布過的索引消失時禁止建立空檔代替。
        directory = volume(self.root,self.binding)
        path = directory/'history.sqlite'
        expected = self.db.execute("SELECT 1 FROM cold_state WHERE name='index'").fetchone()
        if (expected or not create) and not path.is_file():
            raise HistoryUnavailable('cold history index missing')
        identity = (path.stat().st_dev,path.stat().st_ino) if path.exists() else None
        if self.remote and identity != self.file_identity:
            self.remote.close()
            self.remote = None
        if not self.remote:
            handle = sqlite3.connect(path.as_uri()+('?mode=rwc' if create else '?mode=rw'),uri=True,check_same_thread=False)
            if create:
                handle.executescript(INDEX_SCHEMA)
            self.remote = handle
            self.file_identity = (path.stat().st_dev,path.stat().st_ino)
        return self.remote

    @staticmethod
    def short_key(ident):
        # 完整原生身分可留在冷盤，熱盤前綴只用來快速排除。
        return ident[:7] if isinstance(ident,bytes) else hashlib.sha256(ident.encode()).digest()[:7]

    def possible(self, feed, ident):
        # 熱盤保留短指紋；只用陰性結果排除，前綴碰撞一律回冷盤核對完整鍵。
        short = self.short_key(ident)
        return short, self.populated and self.db.execute('SELECT 1 FROM cold_keys WHERE feed=? AND short=?',(feed,short)).fetchone()

    def lookup(self, feed, ident):
        # 查重讀取失敗交給耐久 spool 等待；不得當作新事件發布。
        if not self.possible(feed,ident)[1]:
            return []
        try:
            return self.connect().execute('SELECT seq,digest FROM seen WHERE feed=? AND ident=?',(feed,ident)).fetchall()
        except (OSError,sqlite3.Error) as exc:
            raise HistoryUnavailable(str(exc)) from exc

    def prepare(self,limit):
        # 先在持鎖期間從熱盤取有界快照，慢速的冷盤同步稍後在鎖外執行。
        rows = self.db.execute('SELECT * FROM archive.seen ORDER BY seq LIMIT ?', (limit,)).fetchall()
        if not rows:
            return None
        with self.db:
            self.db.execute('CREATE TEMP TABLE IF NOT EXISTS transferring(seq INTEGER PRIMARY KEY)')
            self.db.execute('DELETE FROM transferring')
            self.db.executemany('INSERT INTO transferring VALUES(?)',[(r[0],) for r in rows])
        names = self.db.execute('SELECT id,name FROM feed_names WHERE id IN '
                                '(SELECT feed FROM archive.seen WHERE seq IN transferring)').fetchall()
        batch = {'seen':rows,'feed_names':names}
        for table in ('variants','edges'):
            batch[table] = self.db.execute(f'SELECT * FROM archive.{table} WHERE seq IN transferring').fetchall()
        return batch

    def copy_index(self,batch):
        # 獨立冷盤連線寫入，副本提交鎖在整段冷盤 I/O 期間保持可用。
        directory = volume(self.root,self.binding)
        path = directory/'history.sqlite'
        if self.populated and not path.is_file():
            raise HistoryUnavailable('cold history index missing')
        with closing(sqlite3.connect(path,timeout=2)) as remote:
            remote.executescript(INDEX_SCHEMA)
            with remote:
                for table,rows in batch.items():
                    if rows:
                        remote.executemany(f'INSERT OR IGNORE INTO {table} VALUES({",".join("?"*len(rows[0]))})',rows)
        sync_directory(directory)

    def finish(self,batch):
        # 確認冷盤仍在後才發布短指紋，兩階段提交可從同一份熱索引重試。
        volume(self.root,self.binding)
        rows = batch['seen']
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO cold_state VALUES('index')")
            self.db.executemany('INSERT OR IGNORE INTO cold_keys VALUES(?,?)',
                                [(r[1],self.short_key(r[2])) for r in rows])
        self.populated = True
        with self.db:
            for table in ('seen','edges','variants'):
                self.db.execute(f'DELETE FROM archive.{table} WHERE seq IN transferring')


    def migrate(self,limit):
        # 離線重播可同步完成三步，線上主管讓冷盤複製獨立運行。
        if self.binding and (batch := self.prepare(limit)):
            self.copy_index(batch)
            self.finish(batch)


INDEX_SCHEMA = """CREATE TABLE IF NOT EXISTS feed_names(id INTEGER PRIMARY KEY,name TEXT UNIQUE);
                  CREATE TABLE IF NOT EXISTS seen(seq INTEGER PRIMARY KEY,feed,ident,digest,strong,received);
                  CREATE INDEX IF NOT EXISTS ids ON seen(feed,ident);
                  CREATE TABLE IF NOT EXISTS variants(seq INTEGER PRIMARY KEY,digest,category);
                  CREATE TABLE IF NOT EXISTS edges(seq INTEGER PRIMARY KEY,feed,u,previous,first,reset);
                  CREATE INDEX IF NOT EXISTS edge_range ON edges(feed,u);"""
