"""用獨立唯讀連線監測兩盤與維護積壓，避免佔用行情寫入鎖。"""
from contextlib import closing
import json
import shutil
import sqlite3
import time
from common.segments import volume


class StorageHealth:
    def __init__(self, root):
        # 分別採樣兩個磁碟的淨成長，至少跨過一小時才提供容量外推。
        self.root = root
        self.samples = {}
        self.latest = {}

    def disk(self, name, path, now):
        # 裝置變更時捨棄舊樣本；釋放空間或沒有淨成長時不估算天數。
        free = shutil.disk_usage(path).free
        device = path.stat().st_dev
        samples = self.samples.setdefault(name, [])
        if samples and device != samples[-1][2]:
            samples.clear()
        if not samples or now-samples[-1][0]>=60:
            samples.append((now,free,device))
            if len(samples)>1441:
                del samples[0]
        start, baseline, _ = samples[0]
        growth = baseline-free
        days = free*(now-start)/growth/86400 if now-start>=3600 and growth>0 else None
        return dict(free_bytes=free,estimated_days=days)

    def read(self, binding):
        # 記錄封段後等待時間、位元組及正常觀察整理積壓。
        now = time.time()
        with closing(sqlite3.connect((self.root/'_state/merge.sqlite').as_uri()+'?mode=ro',uri=True)) as db:
            observations, = db.execute('SELECT count(*) FROM observations WHERE observed<?',(now-86400,)).fetchone()
            pending = db.execute('SELECT count(*),coalesce(sum(bytes),0),min(closed) FROM sealed_at '
                                 'WHERE moved=0 AND closed<=?',(now-86400,)).fetchone()
            transport = [dict(worker=w,feed=f,event=e,details=json.loads(d),local_timestamp=t) for w,f,e,d,t in
                         db.execute('SELECT worker,feed,event,details,received FROM transport_log ORDER BY id DESC LIMIT 50')]
            waiting, = db.execute('SELECT count(*) FROM waiting').fetchone()
        data = dict(recent_connections=transport,observations_past_retention=observations,history_waiting=waiting,
                    hot=self.disk('hot',self.root,now),cold=None, archive_enabled=bool(binding))
        if binding:
            data.update(archive_segments=pending[0],archive_pending_bytes=pending[1],oldest_unarchived_at=pending[2])
            try:
                data['cold'] = self.disk('cold',volume(self.root,binding),now)
            except OSError as exc:
                data['cold_error'] = str(exc)
        self.latest = data
        return data
