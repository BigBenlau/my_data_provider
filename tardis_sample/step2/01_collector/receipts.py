"""冷盤待查時保留連續 ACK，並以稀疏收據記錄其後已正式提交的輸入。"""
from .cold import HistoryUnavailable
from .transport import key

SCHEMA = """
CREATE TABLE IF NOT EXISTS transport_log(id INTEGER PRIMARY KEY,worker TEXT,feed TEXT,event TEXT,details TEXT,received INTEGER);
CREATE TABLE IF NOT EXISTS rotation_decisions(id INTEGER PRIMARY KEY,details TEXT);
CREATE TABLE IF NOT EXISTS scanned(worker TEXT PRIMARY KEY,seq INTEGER);
CREATE TABLE IF NOT EXISTS receipts(worker TEXT,seq INTEGER,PRIMARY KEY(worker,seq)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS waiting(ticket INTEGER PRIMARY KEY,worker TEXT,seq INTEGER,feed TEXT,UNIQUE(worker,seq));
CREATE INDEX IF NOT EXISTS waiting_feed ON waiting(feed,ticket);
CREATE TABLE IF NOT EXISTS sealed_at(name TEXT PRIMARY KEY,closed REAL,moved INTEGER DEFAULT 0,bytes INTEGER DEFAULT 0,digest TEXT);
CREATE INDEX IF NOT EXISTS archive_queue ON sealed_at(closed) WHERE moved=0;
CREATE TRIGGER IF NOT EXISTS segment_clock AFTER INSERT ON segments BEGIN
 INSERT OR IGNORE INTO sealed_at(name,closed) VALUES(new.name,strftime('%s','now')); END;
"""


def defer(store, worker, seq, feed, reason):
    # 同一來源以首次掃描次序等待，已落盤的其他來源可獨立取得收據。
    inserted = store.db.execute('INSERT OR IGNORE INTO waiting(worker,seq,feed) VALUES(?,?,?)',(worker,seq,feed)).rowcount
    if inserted and reason:
        store.notice('cold_history_unavailable',worker=worker,feed=feed,reason=str(reason))


def consume_or_wait(store, worker, item):
    # 可能涉及歷史的整個原始批次原子處理，遇缺盤時連同所有拆分事件回滾。
    seq, record = item['seq'], item['record']
    feed = key(record)
    row = store.db.execute('SELECT worker,seq FROM waiting WHERE feed=? ORDER BY ticket LIMIT 1',(feed,)).fetchone()
    if row and row != (worker,seq):
        defer(store,worker,seq,feed,None)
        return False
    before = store.sequence
    store.db.execute('SAVEPOINT cold_lookup')
    try:
        store.consume(worker,record)
    except HistoryUnavailable as exc:
        store.db.execute('ROLLBACK TO cold_lookup')
        store.sequence = before
        store.evidence.cache.clear()
        defer(store,worker,seq,feed,exc)
        return False
    finally:
        store.db.execute('RELEASE cold_lookup')
    if row:
        store.db.execute('DELETE FROM waiting WHERE worker=? AND seq=?',(worker,seq))
    return True


def process(store, worker, batch, cursor):
    # 已掃描游標不授權刪除 spool；只有連續完成且正式 JSONL 同步後才回覆 ACK。
    row = store.db.execute('SELECT seq FROM scanned WHERE worker=?',(worker,)).fetchone()
    scanned = max(cursor,row[0] if row else 0)
    for item in batch:
        seq = item['seq']
        if seq <= cursor:
            continue
        if seq > scanned+1:
            raise ValueError('worker input sequence gap')
        store.input_seq = seq
        if seq <= scanned and store.db.execute('SELECT 1 FROM receipts WHERE worker=? AND seq=?',(worker,seq)).fetchone():
            continue
        scanned = max(scanned,seq)
        if store.cold.populated:
            if not consume_or_wait(store,worker,item):
                continue
        else:
            store.consume(worker,item['record'])
        if seq == cursor+1:
            cursor = seq
        else:
            store.db.execute('INSERT OR IGNORE INTO receipts VALUES(?,?)',(worker,seq))
    if cursor < scanned:
        first = store.db.execute('SELECT min(seq) FROM waiting WHERE worker=?',(worker,)).fetchone()[0]
        cursor = scanned if first is None else first-1
        store.db.execute('DELETE FROM receipts WHERE worker=? AND seq<=?',(worker,cursor))
    store.db.execute('INSERT OR REPLACE INTO scanned VALUES(?,?)',(worker,scanned))
    return cursor
