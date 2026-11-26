"""用整數路徑保存近期收訊證據，正常明細到期後按 UTC 小時彙總。"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS workers_dict(id INTEGER PRIMARY KEY,name TEXT UNIQUE);
CREATE TABLE IF NOT EXISTS paths(id INTEGER PRIMARY KEY,worker INTEGER,session TEXT,UNIQUE(worker,session));
CREATE TABLE IF NOT EXISTS observations(seq INTEGER,path INTEGER,observed REAL,feed INTEGER,
 strong INTEGER,input_seq INTEGER,ordinal INTEGER,PRIMARY KEY(seq,path,input_seq,ordinal)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS observation_age ON observations(observed,seq);
CREATE INDEX IF NOT EXISTS path_time ON observations(path,feed,observed);
CREATE INDEX IF NOT EXISTS gap_deadline ON gaps(created) WHERE resolved=0;
CREATE INDEX IF NOT EXISTS open_gap_feeds ON gaps(feed) WHERE resolved=2;
CREATE TABLE IF NOT EXISTS hourly(hour INTEGER,feed INTEGER,path INTEGER,received INTEGER,common INTEGER,
 single INTEGER,unverified INTEGER,PRIMARY KEY(hour,feed,path)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS pending_evidence(feed INTEGER,first REAL,last REAL);
CREATE INDEX IF NOT EXISTS pending_source ON pending_evidence(feed,first,last);
CREATE TABLE IF NOT EXISTS rolling(seq INTEGER PRIMARY KEY,category INTEGER);
CREATE TABLE IF NOT EXISTS protected_seq(seq INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS handovers(feed INTEGER,started REAL,ended REAL,token TEXT,PRIMARY KEY(token,feed));
CREATE INDEX IF NOT EXISTS handover_range ON handovers(feed,started,ended);
CREATE INDEX IF NOT EXISTS gap_range ON gaps(feed,start_u,end_u) WHERE resolved<>1;
CREATE TABLE IF NOT EXISTS cold_gap_retry(gap INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS dirty_gaps(gap INTEGER PRIMARY KEY);
CREATE TRIGGER IF NOT EXISTS new_gap AFTER INSERT ON gaps BEGIN
 INSERT OR IGNORE INTO dirty_gaps VALUES(new.rowid); END;
CREATE TRIGGER IF NOT EXISTS relevant_edge AFTER INSERT ON edges BEGIN
 INSERT OR IGNORE INTO dirty_gaps SELECT rowid FROM gaps WHERE resolved<>1 AND feed=new.feed
 AND start_u<new.u AND end_u>=new.u; END;
"""


class Evidence:
    def __init__(self, db):
        # 路徑字典只快取本次程序已見的組合，交易回滾時同步清除。
        self.db = db
        db.executescript(SCHEMA)
        self.cache = {}

    def path(self, worker, session):
        # 同一副本交接時有兩個 session，兩路的共同事件須能各自辨認。
        pair = (worker, session)
        if pair not in self.cache:
            self.db.execute('INSERT OR IGNORE INTO workers_dict(name) VALUES(?)', (worker,))
            wid, = self.db.execute('SELECT id FROM workers_dict WHERE name=?', (worker,)).fetchone()
            self.db.execute('INSERT OR IGNORE INTO paths(worker,session) VALUES(?,?)', (wid, session))
            self.cache[pair], = self.db.execute('SELECT id FROM paths WHERE worker=? AND session=?', (wid, session)).fetchone()
        return self.cache[pair]

    def record(self, seq, worker, event, feed, input_seq, duplicate):
        # 彙總後首次收到的遲到副本，其歷史覆蓋無法重建。
        rec = event['record']
        verified = int(event['strong'])
        if duplicate and not self.db.execute('SELECT 1 FROM observations WHERE seq=? AND NOT EXISTS(SELECT 1 FROM rolling WHERE seq=?) LIMIT 1', (seq,seq)).fetchone():
            verified = -1
        self.db.execute('INSERT OR IGNORE INTO observations VALUES(?,?,?,?,?,?,?)',
                        (seq, self.path(worker, rec.get('session', '')), rec['local_timestamp']/1e6,
                         feed, verified, input_seq,rec.get('batch_index',0)))

    def common(self, feed, paths, since, needed):
        # 從最新路徑的有界時間窗尋找同一正式序號在所有指定 session 的證據。
        ids = [self.cache.get((w,s)) for w,s in paths]
        if None in ids:
            rows = [self.db.execute('SELECT p.id FROM paths p JOIN workers_dict w ON w.id=p.worker '
                                    'WHERE w.name=? AND p.session=?',pair).fetchone() for pair in paths]
            if not all(rows):
                return False
            ids = [r[0] for r in rows]
        if len(set(ids)) != len(paths):
            return False
        tests = ' AND '.join('EXISTS(SELECT 1 FROM observations b WHERE b.seq=a.seq AND b.path=? AND b.strong=1 AND b.observed>=?)' for _ in ids[1:])
        sql = f'SELECT DISTINCT a.seq FROM observations a WHERE a.observed>=? AND a.feed=? AND a.path=? AND a.strong=1 AND {tests} LIMIT ?'
        checks = [argument for path in ids[1:] for argument in (path,since)]
        return len(self.db.execute(sql,(since,feed,ids[0],*checks,needed)).fetchall()) >= needed

    def summarize(self, cutoff, limit):
        # 先篩選可整理的事件，彙總與刪除明細在同一次交易內完成。
        db = self.db
        db.execute('CREATE TEMP TABLE IF NOT EXISTS eligible(seq,path,input_seq,ordinal,PRIMARY KEY(seq,path,input_seq,ordinal)) WITHOUT ROWID')
        db.execute('DELETE FROM eligible')
        db.execute("""INSERT INTO eligible SELECT o.seq,o.path,o.input_seq,o.ordinal FROM observations o
            WHERE o.observed<? AND NOT EXISTS(SELECT 1 FROM protected_seq p WHERE p.seq=o.seq)
            AND NOT EXISTS(SELECT 1 FROM observations v JOIN paths p ON p.id=v.path
              JOIN workers_dict w ON w.id=p.worker LEFT JOIN inputs i ON i.worker=w.name
              WHERE v.seq=o.seq AND (v.observed>=? OR v.input_seq>coalesce(i.cursor,0)
              OR EXISTS(SELECT 1 FROM pending_evidence e WHERE e.feed=v.feed AND v.observed BETWEEN e.first AND e.last)
              OR EXISTS(SELECT 1 FROM handovers h WHERE h.feed=v.feed AND v.observed>=h.started
                  AND (h.ended IS NULL OR v.observed<=h.ended))))
            AND NOT EXISTS(SELECT 1 FROM edges e JOIN gaps g ON g.feed=e.feed AND g.resolved<>1
              AND e.u BETWEEN g.start_u AND g.end_u WHERE e.seq=o.seq)
            LIMIT ?""", (cutoff,cutoff,limit))
        db.execute("""INSERT OR IGNORE INTO rolling SELECT seq,CASE WHEN min(strong)<>1 THEN -1
            WHEN count(DISTINCT p.worker)>1 THEN 2 ELSE 1 END FROM observations JOIN paths p ON p.id=path
            WHERE seq IN (SELECT seq FROM eligible) GROUP BY seq""")
        db.execute("""INSERT INTO hourly SELECT cast(observed/3600 AS INTEGER),feed,path,count(*),
            sum(category=2),sum(category=1),sum(category<0) FROM eligible JOIN observations
            USING(seq,path,input_seq,ordinal) JOIN rolling USING(seq) GROUP BY 1,2,3
            ON CONFLICT(hour,feed,path) DO UPDATE SET received=received+excluded.received,
            common=common+excluded.common,single=single+excluded.single,unverified=unverified+excluded.unverified""")
        removed = db.execute('DELETE FROM observations WHERE (seq,path,input_seq,ordinal) IN eligible').rowcount
        db.execute('DELETE FROM rolling WHERE seq IN (SELECT seq FROM eligible) AND NOT EXISTS '
                   '(SELECT 1 FROM observations o WHERE o.seq=rolling.seq)')
        return removed
