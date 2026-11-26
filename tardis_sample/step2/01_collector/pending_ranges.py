"""以每來源的小型交付區間追蹤尚未排空的耐久 spool。"""
from contextlib import closing
import sqlite3


def register(db):
    # 輔助範圍與 payload 一起提交，主管不必掃描大量 JSON 原文。
    db.execute('CREATE TABLE IF NOT EXISTS pending_ranges(feed TEXT,block INTEGER,last INTEGER,first_us INTEGER,'
               'last_us INTEGER,PRIMARY KEY(block,feed)) WITHOUT ROWID')


def record_ranges(db, batch):
    # 每 1024 筆共用區間；保守保留交疊到的整個區間直到其最後一筆確認。
    bounds = {}
    for seq,feed,stamp in batch:
        name = (seq//1024,feed)
        first,last = bounds.get(name,(stamp,stamp))[:2]
        bounds[name] = (min(first,stamp),max(last,stamp),seq)
    db.executemany('INSERT INTO pending_ranges(block,feed,first_us,last_us,last) VALUES(?,?,?,?,?) '
                   'ON CONFLICT(block,feed) DO UPDATE SET last=excluded.last,first_us=min(first_us,excluded.first_us),'
                   'last_us=max(last_us,excluded.last_us)',
                   [(*name,*values) for name,values in bounds.items()])


def collect(root, cutoff):
    # 僅讀取可能妨礙 24 小時彙總的範圍；失敗時本輪保留明細。
    rows = []
    for path in root.glob('*/spool.sqlite'):
        try:
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.25)) as db:
                if not db.execute("SELECT 1 FROM sqlite_master WHERE name='pending_ranges'").fetchone():
                    continue
                rows.extend(db.execute('SELECT feed,min(first_us)/1e6,max(last_us)/1e6 FROM pending_ranges '
                                       'WHERE first_us<? GROUP BY feed',(int(cutoff*1e6),)))
        except sqlite3.OperationalError:
            if path.exists():
                raise
    return rows
