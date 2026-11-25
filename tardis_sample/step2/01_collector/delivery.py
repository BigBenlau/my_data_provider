"""待交付緩衝與 supervisor 提交日誌；確認必須晚於正式 JSONL 持久化。"""
from collections import defaultdict, deque
import json
import logging
import os
from pathlib import Path
import queue
import shutil
import sqlite3
import threading
import time
import asyncio
import struct
from common import LOG, utc
from .transport import key
from .raw import StorageError
from .redundancy import stable_json, source_inventory
from .replica_protocol import atoms
from .retention import IndexArchive, sync_directory


async def read_packet(reader):
    # 長度前綴避免大型 depth 訊息碰到逐行讀取限制，並限制單封包記憶體。
    size = struct.unpack('!I', await reader.readexactly(4))[0]
    if size > 32 * 1024**2: raise ValueError('IPC packet too large')
    return json.loads(await reader.readexactly(size))


async def write_packet(writer, value):
    # 排程器與網路收訊分開；IPC 壅塞只延後交付，不丟棄 spool。
    data = stable_json(value).encode()
    writer.write(struct.pack('!I', len(data)) + data)
    await writer.drain()


def database(path):
    # WAL 只使用主機本地磁碟，完整同步保護程序崩潰後的游標。
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, timeout=30, check_same_thread=False)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.execute('PRAGMA busy_timeout=30000')
    # 維持每次提交完整同步，延後主檔 checkpoint 以合併重複索引頁寫入。
    db.execute('PRAGMA wal_autocheckpoint=16384')
    # spool 的讀取、刪除與背景追加也需要頁面快取，避免小快取來回換頁。
    db.execute('PRAGMA cache_size=-65536')
    return db


def disk_guard(path, reserve):
    # 寫入前檢查保留空間；不足時停止而不是刪除未確認資料。
    if shutil.disk_usage(path).free < reserve * 1024**3:
        raise StorageError('free disk below configured reserve')


class Spool:
    """worker 的暫存佇列；背景批次落盤後才可送出，ACK 後刪除。"""
    def __init__(self, root, options):
        # 復用同 worker 的序號；容器重啟不覆蓋未交付內容。
        self.root = Path(root); self.root.mkdir(parents=True, exist_ok=True)
        self.options = options; self.lock = threading.RLock()
        self.memory_lock = threading.Lock(); self.queued_bytes = 0
        self.db = database(self.root / 'spool.sqlite')
        self.db.executescript('CREATE TABLE IF NOT EXISTS items(seq INTEGER PRIMARY KEY,payload TEXT);'
                             'CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY,seq INTEGER,ack INTEGER);'
                             'INSERT OR IGNORE INTO state VALUES(1,0,0);')
        self.seq, self.ack = self.db.execute('SELECT seq,ack FROM state').fetchone()
        self.written = self.seq; self.error = None; self.high_water = 0
        self.bytes = self.db.execute('SELECT coalesce(sum(length(cast(payload AS BLOB))),0) FROM items').fetchone()[0]
        self.oldest_us = 0; self.last_checkpoint = time.monotonic(); self.refresh_oldest()
        self.queue = queue.Queue(100000); self.last_write = time.monotonic()
        self.thread = threading.Thread(target=self.loop, daemon=True, name='durable-spool')
        self.thread.start()

    def emit(self, record):
        # 不在收訊主循環執行 fsync；容量滿時立即回報致命儲存錯誤。
        if self.error: raise StorageError(str(self.error))
        self.seq += 1
        record = dict(record, delivery_sequence=self.seq)
        payload = stable_json(record)
        with self.memory_lock:
            self.queued_bytes += len(payload.encode())
            if self.queued_bytes > self.options.get('memory_queue_bytes', 64 * 1024**2):
                raise StorageError('spool memory byte limit exceeded')
        try: self.queue.put_nowait((self.seq, payload))
        except queue.Full as exc: raise StorageError('spool memory queue full') from exc
        self.high_water = max(self.high_water, self.queue.qsize())
        return self.seq

    def write(self, batch):
        # 序號與 payload 在同一交易提交，禁止傳送尚未持久化的資料。
        disk_guard(self.root, self.options['min_free_gib'])
        size = sum(len(p.encode()) for _, p in batch)
        with self.lock:
            if self.bytes + size > self.options['spool_max_bytes']:
                raise StorageError('unacknowledged spool exceeds capacity')
            with self.db:
                self.db.executemany('INSERT INTO items VALUES(?,?)', batch)
                self.db.execute('UPDATE state SET seq=? WHERE id=1', (batch[-1][0],))
            self.written = batch[-1][0]; self.bytes += size
            if not self.oldest_us: self.refresh_oldest()
            self.last_write = time.monotonic()
        with self.memory_lock: self.queued_bytes -= size

    def loop(self):
        # 將短時間內收到的訊息合成一筆磁碟交易；任何錯誤都通知主程序。
        batch = []; deadline = time.monotonic() + self.options['flush_seconds']
        try:
            while True:
                try: item = self.queue.get(timeout=max(.001, deadline - time.monotonic()))
                except queue.Empty: item = 'flush'
                if item is None:
                    if batch: self.write(batch)
                    return
                if item != 'flush': batch.append(item)
                if len(batch) >= self.options['batch_size'] or time.monotonic() >= deadline:
                    if batch: self.write(batch); batch = []
                    deadline = time.monotonic() + self.options['flush_seconds']
        except BaseException as exc:
            self.error = exc
            LOG.exception('event=spool_failed root=%s', self.root)

    def pending(self):
        # 傳送器只讀已提交的批次，限制單次訊息大小。
        rows = []; size = 0
        with self.lock:
            cursor = self.db.execute('SELECT seq,payload FROM items ORDER BY seq LIMIT ?', (self.options['batch_size'],))
            for row in cursor:
                length = len(row[1].encode())
                if rows and size + length > self.options.get('batch_max_bytes', 16 * 1024**2): break
                rows.append(row); size += length
        return [{'seq': n, 'record': json.loads(p)} for n, p in rows]

    def acknowledge(self, sequence):
        # ACK 只能前進到已落盤位置；清理 payload 但保留持久序號。
        with self.lock:
            if sequence < self.ack or sequence > self.written:
                raise ValueError('invalid supervisor acknowledgement')
            # 只計算本次確認的前綴，避免積壓越大、每次確認越慢。
            released = self.db.execute('SELECT coalesce(sum(length(cast(payload AS BLOB))),0) FROM items WHERE seq<=?', (sequence,)).fetchone()[0]
            with self.db:
                self.db.execute('DELETE FROM items WHERE seq<=?', (sequence,))
                self.db.execute('UPDATE state SET ack=? WHERE id=1', (sequence,))
            self.ack = sequence
            self.bytes -= released; self.refresh_oldest()
            if not self.bytes and time.monotonic() - self.last_checkpoint >= 60:
                self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)'); self.last_checkpoint = time.monotonic()

    def refresh_oldest(self):
        # 快取佇列首筆的接收時間；健康回報不另外查詢或等待資料庫鎖。
        row = self.db.execute('SELECT payload FROM items ORDER BY seq LIMIT 1').fetchone()
        self.oldest_us = json.loads(row[0]).get('local_timestamp', 0) if row else 0

    def close(self):
        # 排空背景寫入，保留尚未 ACK 的磁碟資料供下次接續。
        while self.thread.is_alive() and not self.error:
            try: self.queue.put(None, timeout=.1); break
            except queue.Full: pass
        self.thread.join(timeout=60)
        if self.thread.is_alive(): raise StorageError('spool drain timeout')
        if self.error: raise StorageError(str(self.error))


SCHEMA = '''
CREATE TABLE IF NOT EXISTS inputs(worker TEXT PRIMARY KEY,cursor INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS seen(seq INTEGER PRIMARY KEY,feed INTEGER,ident BLOB,digest BLOB,strong INTEGER,received INTEGER);
CREATE TABLE IF NOT EXISTS feed_names(id INTEGER PRIMARY KEY,name TEXT NOT NULL UNIQUE);
CREATE INDEX IF NOT EXISTS event_lookup ON seen(feed,ident,digest);
CREATE TABLE IF NOT EXISTS observations(seq INTEGER,worker TEXT,session TEXT,observed REAL,PRIMARY KEY(seq,worker));
CREATE INDEX IF NOT EXISTS recent_observation ON observations(worker,observed);
CREATE TABLE IF NOT EXISTS outbox(seq INTEGER PRIMARY KEY,payload TEXT);
CREATE TABLE IF NOT EXISTS output(id INTEGER PRIMARY KEY,seq INTEGER,written INTEGER,path TEXT,offset INTEGER,opened REAL);
INSERT OR IGNORE INTO output VALUES(1,0,0,NULL,0,0);
CREATE TABLE IF NOT EXISTS notices(id INTEGER PRIMARY KEY AUTOINCREMENT,level TEXT,body TEXT);
CREATE TABLE IF NOT EXISTS worker_state(worker TEXT PRIMARY KEY,status TEXT,updated REAL);
CREATE TABLE IF NOT EXISTS markers(worker TEXT,feed TEXT,session TEXT,u INTEGER,PRIMARY KEY(worker,feed));
CREATE TABLE IF NOT EXISTS gaps(worker TEXT,feed TEXT,start_u INTEGER,end_u INTEGER,created REAL,resolved INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS edges(seq INTEGER PRIMARY KEY,feed TEXT,u INTEGER,previous INTEGER,first INTEGER,reset INTEGER);
CREATE INDEX IF NOT EXISTS edge_lookup ON edges(feed,u);
CREATE TABLE IF NOT EXISTS conflicts(feed TEXT,created REAL,seq INTEGER);
CREATE TABLE IF NOT EXISTS totals(name TEXT PRIMARY KEY,value INTEGER);
CREATE TABLE IF NOT EXISTS snapshot_requests(gap INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS variants(seq INTEGER PRIMARY KEY,digest TEXT,category TEXT);
CREATE TABLE IF NOT EXISTS segments(name TEXT PRIMARY KEY,first_seq INTEGER,last_seq INTEGER,compressed INTEGER DEFAULT 0);
CREATE INDEX IF NOT EXISTS segment_cursor ON segments(last_seq);
CREATE INDEX IF NOT EXISTS raw_compression_queue ON segments(first_seq) WHERE compressed=0;
CREATE TABLE IF NOT EXISTS maintenance(name TEXT PRIMARY KEY,value TEXT);
'''


class MergeStore:
    """唯一合併資料庫；JSONL 提交後才確認輸入，不永久保存副本原文。"""
    def __init__(self, root, config):
        # 先恢復未完成提交，再允許 worker 取得 ACK。
        self.root = Path(root); self.options = config['redundancy']; self.config = config
        (self.root / 'raw').mkdir(parents=True, exist_ok=True)
        self.db = database(self.root / '_state' / 'merge.sqlite')
        # 隨機事件索引需要足夠快取；避免容器 bind mount 頻繁讀取索引頁。
        self.db.execute('PRAGMA cache_size=-65536')
        self.db.executescript(SCHEMA); self.counters = {}; self.metrics = {}
        self.samples = defaultdict(lambda: deque(maxlen=4 * self.options['match_events']))
        self.prepare_keys()
        self.recover_output(); self.flush()
        self.archive = IndexArchive(self.db, self.root)
        self.register_segments()

    def prepare_keys(self):
        # 新 run 使用緊湊鍵；開啟既有文字表時保留原編碼與歷史索引。
        types = {row[1]: row[2] for row in self.db.execute('PRAGMA table_info(seen)')}
        self.compact = types['feed'].upper() == 'INTEGER'
        self.feed_ids = {}
        if self.compact:
            with self.db:
                self.db.executemany('INSERT OR IGNORE INTO feed_names(name) VALUES(?)',
                                    [(name,) for name in sorted(source_inventory(self.config))])
            self.feed_ids = dict(self.db.execute('SELECT name,id FROM feed_names'))

    def feed_key(self, feed):
        # 常見來源從已提交的字典讀取；額外來源與事件同交易，回滾不會留下快取。
        if not self.compact:
            return feed
        if feed in self.feed_ids:
            return self.feed_ids[feed]
        self.db.execute('INSERT OR IGNORE INTO feed_names(name) VALUES(?)', (feed,))
        return self.db.execute('SELECT id FROM feed_names WHERE name=?', (feed,)).fetchone()[0]

    def packed_key(self, value):
        # 只轉換可逐字還原的小寫十六進位；其他 REST 身分保留原字串及大小寫。
        if self.compact and isinstance(value, str) and len(value) in (32, 64):
            try:
                packed = bytes.fromhex(value)
            except ValueError:
                return value
            if packed.hex() == value:
                return packed
        return value

    def observed_feeds(self):
        # 歷史與熱索引共用永久來源字典；僅回報真的有行情的來源。
        query = 'SELECT DISTINCT feed FROM seen UNION SELECT DISTINCT feed FROM archive.seen'
        if self.compact:
            query = f'SELECT name FROM feed_names WHERE id IN ({query})'
        return {row[0] for row in self.db.execute(query)}

    def register_segments(self):
        # 舊版本只在首次升級時掃描封存段；之後 CSV 直接查詢持久目錄。
        if self.db.execute("SELECT 1 FROM maintenance WHERE name='segment_catalog'").fetchone(): return
        names = sorted({p.name.removesuffix('.gz') for p in (self.root / 'raw').glob('*.jsonl*')
                        if p.name.endswith(('.jsonl', '.jsonl.gz'))})
        active, written = self.db.execute('SELECT path,written FROM output').fetchone()
        end = int(Path(active).name.split('_')[0]) - 1 if active else written
        with self.db:
            for index, name in enumerate(names):
                last = int(names[index + 1].split('_')[0]) - 1 if index + 1 < len(names) else end
                self.db.execute('INSERT OR IGNORE INTO segments(name,first_seq,last_seq) VALUES(?,?,?)', (name, int(name.split('_')[0]), last))
            self.db.execute("INSERT INTO maintenance VALUES('segment_catalog','1')")

    def recover_output(self):
        # 封存改名與游標提交之間崩潰時補登目錄；已確認原檔消失必須報錯。
        path, offset, written = self.db.execute('SELECT path,offset,written FROM output').fetchone()
        if not path: return
        current = self.root / path; sealed = current.with_suffix('')
        if current.exists():
            if current.stat().st_size < offset: raise StorageError('committed JSONL truncated')
            return
        if not sealed.exists():
            if offset: raise StorageError('committed JSONL missing')
            return
        if sealed.stat().st_size != offset: raise StorageError('sealed JSONL size mismatch')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO segments(name,first_seq,last_seq) VALUES(?,?,?)',
                            (sealed.name, int(sealed.name.split('_')[0]), written))
            self.db.execute('UPDATE output SET path=NULL,offset=0 WHERE id=1')

    def notice(self, event, **fields):
        # 狀態與缺口先進交易日誌；不把副本 payload 放到運行日誌。
        self.db.execute('INSERT INTO notices(level,body) VALUES(?,?)',
                        ('INFO' if event in ('connect_attempt', 'first_message') else 'WARNING', stable_json(dict(event=event, **fields))))

    def drain_notices(self):
        # 日誌可能在崩潰邊界重複，但不能遺失已提交的故障通知。
        rows = self.db.execute('SELECT * FROM notices ORDER BY id').fetchall()
        for ident, level, body in rows:
            LOG.log(getattr(logging, level), 'event=replica_notice details=%s', body)
        # 每批只做一次日誌游標交易，避免異常較多時每行都觸發磁碟同步。
        if rows:
            with self.db: self.db.execute('DELETE FROM notices WHERE id<=?', (rows[-1][0],))

    def ingest(self, worker, batch):
        # 一批輸入先記錄去重決策與待寫內容；ACK 晚於 flush。
        disk_guard(self.root, self.options['min_free_gib'])
        started = time.monotonic()
        try:
            with self.db:
                self.sequence = self.db.execute('SELECT seq FROM output').fetchone()[0]
                self.db.execute('INSERT OR IGNORE INTO inputs VALUES(?,0)', (worker,))
                cursor = self.db.execute('SELECT cursor FROM inputs WHERE worker=?', (worker,)).fetchone()[0]
                for item in batch:
                    if item['seq'] <= cursor: continue
                    if item['seq'] != cursor + 1: raise ValueError('worker input sequence gap')
                    self.consume(worker, item['record'])
                    cursor = item['seq']
                self.db.execute('UPDATE inputs SET cursor=? WHERE worker=?', (cursor, worker))
                self.db.execute('UPDATE output SET seq=? WHERE id=1', (self.sequence,))
            indexed = time.monotonic()
            self.flush()
            flushed = time.monotonic()
            self.drain_notices()
            self.metrics = dict(batch_records=len(batch), index_ms=round((indexed-started)*1000, 3),
                                persist_ms=round((flushed-indexed)*1000, 3), batch_ms=round((time.monotonic()-started)*1000, 3))
            return cursor
        except (OSError, sqlite3.Error) as exc:
            raise StorageError(str(exc)) from exc

    def consume(self, worker, record):
        # 連線事件只進狀態日誌；正式輸出只包含原生行情及 REST 回應。
        if record.get('kind') == 'event':
            self.notice(record.get('event', 'worker_event'), worker=worker,
                        feed=key(record), details=record.get('details', {}))
            return
        try: events = atoms(record)
        except (ValueError, KeyError, TypeError) as exc:
            self.notice('invalid_payload', worker=worker, feed=key(record), reason=str(exc))
            self.stage(dict(record, identity_unverified=True, validation_error=str(exc)))
            return
        for event in events:
            self.observe_marker(worker, event)
            self.merge(worker, event)

    def identities(self, strong, feed_id, ident, fingerprint):
        # 同一次索引查詢辨識相同原文與變體；無歷史索引時省去空資料庫查詢。
        if not strong: return None, None
        conflict = None
        tables = ('seen', 'archive.seen') if self.archive.has_seen else ('seen',)
        for table in tables:
            rows = self.db.execute(f'SELECT seq,digest FROM {table} WHERE feed=? AND ident=?',
                                   (feed_id, ident))
            for sequence, digest in rows:
                if digest == fingerprint: return sequence, None
                if conflict is None: conflict = (sequence,)
        return None, conflict

    def merge(self, worker, event):
        # 衝突版本保留；只有內容相同的已知副本才省略輸出。
        record = event['record']; feed = key(record)
        feed_id = self.feed_key(feed)
        ident, digest = self.packed_key(event['ident']), self.packed_key(event['digest'])
        existing, conflict = self.identities(event['strong'], feed_id, ident, digest)
        if not event['strong']: record = dict(record, identity_unverified=True)
        if existing is None:
            if not event['strong']:
                self.db.execute("INSERT INTO totals VALUES('identity_unverified',1) ON CONFLICT(name) DO UPDATE SET value=value+1")
            # 合併來源跨 worker session 持續觀察，兩路同時重連也不能隱藏缺口。
            if event['marker']:
                merged = dict(event, record=dict(record, session='merged'))
                self.observe_marker('__merged__', merged)
            if conflict:
                category = self.variant_category(event, conflict[0])
                self.notice('content_conflict', worker=worker, feed=feed, previous_seq=conflict[0], category=category)
                record = dict(record, conflict_category=category)
                self.db.execute('INSERT INTO conflicts VALUES(?,?,?)', (feed, time.time(), conflict[0]))
            existing = self.stage(record, bool(conflict))
            self.db.execute('INSERT INTO seen VALUES(?,?,?,?,?,?)',
                            (existing, feed_id, ident, digest, event['strong'], record['local_timestamp']))
            if event.get('variant'):
                v = event['variant']; self.db.execute('INSERT INTO variants VALUES(?,?,?)', (existing, v['digest'], v['category']))
            if event['marker']:
                m = event['marker']
                self.db.execute('INSERT INTO edges VALUES(?,?,?,?,?,?)',
                                (existing, feed, m['u'], m['previous'], m['first'], m['reset']))
        elif event['strong']:
            # 只抽取已遇過的序號；監測另以已提交的兩個 session 觀察列核實。
            self.samples[feed].append(existing)
        self.db.execute('INSERT OR IGNORE INTO observations VALUES(?,?,?,?)',
                        (existing, worker, record.get('session', ''), time.time()))

    def variant_category(self, event, previous):
        # 舊索引沒有分類指紋時仍視為一般衝突，不推測未保存的欄位。
        variant = event.get('variant')
        old = self.db.execute('SELECT digest,category FROM variants WHERE seq=?', (previous,)).fetchone()
        if not old: old = self.db.execute('SELECT digest,category FROM archive.variants WHERE seq=?', (previous,)).fetchone()
        if variant and old and old[0] == variant['digest']: return variant['category']
        return 'native_content_conflict'

    def stage(self, record, conflict=False):
        # 對外 session 是邏輯市場／商品，不隨 A/B 換代；原生內容與收訊時間不變。
        self.sequence += 1; sequence = self.sequence
        clean = {k: v for k, v in record.items() if k not in ('worker_id', 'boot_id', 'seq')}
        clean.update(seq=sequence, session=f'merged:{record["exchange"]}:{record["symbol"]}',
                     merged=True, merge_schema_version=3, source_session=record.get('session'))
        if conflict: clean['content_conflict'] = True
        self.db.execute('INSERT INTO outbox VALUES(?,?)', (sequence, stable_json(clean)))
        return sequence

    def observe_marker(self, worker, event):
        # 只使用 adapter 宣告的前序規則；不建立訂單簿或推測無連續語義的 ID。
        marker = event['marker']
        if marker is None: return
        record = event['record']; feed = key(record); session = record.get('session', '')
        old = self.db.execute('SELECT session,u FROM markers WHERE worker=? AND feed=?', (worker, feed)).fetchone()
        if worker == '__merged__' and old and event['snapshot']: return
        if old and old[0] == session and not marker['reset'] and marker['u'] > old[1]:
            gap = marker['previous'] is not None and marker['previous'] != old[1]
            if marker['previous'] is None and marker['first'] is not None: gap = marker['first'] > old[1] + 1
            if gap:
                self.db.execute('INSERT INTO gaps(worker,feed,start_u,end_u,created) VALUES(?,?,?,?,?)',
                                (worker, feed, old[1], marker['u'], time.time()))
                self.notice('worker_sequence_gap', worker=worker, feed=feed, previous=old[1], current=marker['u'])
        if not old or old[0] != session or marker['reset'] or marker['u'] > old[1]:
            self.db.execute('INSERT OR REPLACE INTO markers VALUES(?,?,?,?)', (worker, feed, session, marker['u']))

    def output_path(self):
        # 先建立可恢復的檔名與偏移；崩潰後截掉未提交尾部再重寫 outbox。
        _, written, path, offset, opened = self.db.execute('SELECT seq,written,path,offset,opened FROM output').fetchone()
        if path and not (self.root / path).exists() and (self.root / path).with_suffix('').exists():
            with self.db: self.db.execute('UPDATE output SET path=NULL,offset=0 WHERE id=1')
            path = None
        if path and time.time() - opened >= self.options['segment_seconds']:
            self.seal(); path = None
        if not path:
            path = f'raw/{written + 1:012d}_{utc(time.time_ns() // 1000, "%Y-%m-%dT%H%M%S")}.jsonl.part'
            with self.db:
                self.db.execute('UPDATE output SET path=?,offset=0,opened=? WHERE id=1', (path, time.time()))
            offset = 0
        return self.root / path, offset

    def flush(self):
        # 先 fsync JSONL，再提交檔案偏移及刪除暫存 payload；此後才允許 ACK。
        rows = self.db.execute('SELECT seq,payload FROM outbox ORDER BY seq').fetchall()
        if not rows: return
        path, offset = self.output_path()
        with path.open('r+b' if path.exists() else 'w+b') as handle:
            handle.truncate(offset); handle.seek(offset)
            for _, payload in rows: handle.write((payload + '\n').encode())
            handle.flush(); os.fsync(handle.fileno()); position = handle.tell()
        with self.db:
            self.db.execute('UPDATE output SET written=?,offset=? WHERE id=1', (rows[-1][0], position))
            self.db.execute('DELETE FROM outbox WHERE seq<=?', (rows[-1][0],))

    def sealed_cursor(self):
        # 靜默期間仍封存已到期的最後一段，按需匯出不必等待下一筆行情。
        path, opened = self.db.execute('SELECT path,opened FROM output').fetchone()
        if path and time.time() - opened >= self.options['segment_seconds']:
            self.seal()
        return self.db.execute('SELECT coalesce(max(last_seq),0) FROM segments').fetchone()[0]

    def seal(self):
        # 完成段才能交給背景 CSV；已提交偏移以外的尾端不可發布。
        path, offset = self.db.execute('SELECT path,offset FROM output').fetchone()
        if not path: return
        target = self.root / path
        if target.exists():
            with target.open('r+b') as handle:
                handle.truncate(offset); handle.flush(); os.fsync(handle.fileno())
            os.replace(target, target.with_suffix(''))
            sync_directory(target.parent)
        with self.db:
            written = self.db.execute('SELECT written FROM output').fetchone()[0]
            name = target.with_suffix('').name
            self.db.execute('INSERT OR IGNORE INTO segments(name,first_seq,last_seq) VALUES(?,?,?)', (name, int(name.split('_')[0]), written))
            self.db.execute('UPDATE output SET path=NULL,offset=0 WHERE id=1')

    def cursor(self, worker):
        # 只在已恢復 outbox 的 store 對外提供已確認游標。
        row = self.db.execute('SELECT cursor FROM inputs WHERE worker=?', (worker,)).fetchone()
        return row[0] if row else 0

    def close(self):
        # 正常停止時完成最後一段，保留索引讓下一個 supervisor 接續。
        self.flush(); self.seal(); self.drain_notices(); self.db.close()
