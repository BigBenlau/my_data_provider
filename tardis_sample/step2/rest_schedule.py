"""同出口的持久化 REST／連線配額與請求租約；worker 換代不重置預算。"""
import math
import time
import uuid
import asyncio
import threading
from delivery import database
from redundancy import stable_json
from common import adapter


class Schedule:
    """由所有 worker 共用的排程；SQLite 交易只包配額計算，不包網路請求。"""
    def __init__(self, path, config):
        # 保存配置上限，實際 exchangeInfo 只能降低允許量，不越過使用者預算。
        self.config = config; self.db = database(path); self.lock = threading.RLock()
        self.epoch = time.time(); self.monotonic = time.monotonic()
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS usage(scope TEXT,at REAL,weight INTEGER,regular INTEGER);
        CREATE INDEX IF NOT EXISTS usage_scope ON usage(scope,at);
        CREATE TABLE IF NOT EXISTS cooldown(scope TEXT PRIMARY KEY,until REAL,used INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,market TEXT,symbol TEXT,channel TEXT,
            due REAL,interval REAL,owner TEXT,lease REAL,request_id TEXT,priority INTEGER);
        CREATE TABLE IF NOT EXISTS limits(market TEXT PRIMARY KEY,capacity INTEGER,verified INTEGER,raw TEXT);
        CREATE TABLE IF NOT EXISTS routes(id TEXT,endpoint TEXT,good INTEGER,cooldown REAL,PRIMARY KEY(id,endpoint));
        CREATE TABLE IF NOT EXISTS rate_windows(market TEXT,kind TEXT,period INTEGER,capacity INTEGER,
            PRIMARY KEY(market,kind,period));
        CREATE TABLE IF NOT EXISTS progress(id TEXT PRIMARY KEY,last_success REAL);
        ''')
        self.initialize()

    async def call(self, method, *args, **kwargs):
        # 跨程序鎖競爭與磁碟同步不得阻塞行情收訊；同一連線仍序列化。
        return await asyncio.to_thread(self.serial, method, args, kwargs)

    def serial(self, method, args, kwargs):
        # 背景工作可能在協程取消後完成；關閉也必須取得相同鎖。
        with self.lock: return getattr(self, method)(*args, **kwargs)

    def now(self):
        # 程序內以單調時鐘推進，持久租約仍使用可跨程序解讀的 epoch。
        return self.epoch + time.monotonic() - self.monotonic

    def initialize(self):
        # 初次才建立任務；重新啟動或候選 worker 不新增同一商品的輪詢。
        with self.db:
            for market, settings in self.config['market_settings'].items():
                if not market.startswith('binance'): continue
                if not set(settings['channels']) & {'depthSnapshot', 'openInterest'}: continue
                cap = self.config['rest_weight_per_minute'].get(market, 1200)
                self.db.execute('INSERT OR IGNORE INTO limits VALUES(?,?,0,NULL)', (market, cap))
                self.add_job(market, '', 'exchangeInfo', 3600, 0)
                for index, symbol in enumerate(settings['symbols']):
                    for channel in ('depthSnapshot', 'openInterest'):
                        if channel in settings['channels']:
                            interval = self.interval(market, channel)
                            phase = index / max(1, len(settings['symbols'])) * interval
                            self.add_job(market, symbol, channel, interval, phase)

    def interval(self, market, channel):
        # 例行使用七成；期貨的 OI／快照再分配六成與四成，並向上取整。
        cap = self.db.execute('SELECT capacity FROM limits WHERE market=?', (market,)).fetchone()[0]
        settings = self.config['market_settings'][market]; count = len(settings['symbols'])
        both = 'openInterest' in settings['channels'] and 'depthSnapshot' in settings['channels']
        share = (.6 if channel == 'openInterest' else .4) if both else 1
        cost = 1 if channel == 'openInterest' else (50 if market == 'binance' else 20)
        minimum = self.config['oi_interval'] if channel == 'openInterest' else self.config['snapshot_interval']
        unit = 5 if channel == 'openInterest' else 60
        return max(minimum, math.ceil(count * cost * 60 / (cap * .7 * share) / unit) * unit)

    def add_job(self, market, symbol, channel, interval, phase):
        # 啟動快照均勻展開；第一輪也標記 warming，不以 ACK 冒充來源完成。
        name = '/'.join((market, symbol, channel))
        delay = phase / interval * 5 if channel == 'depthSnapshot' else phase
        self.db.execute('INSERT OR IGNORE INTO jobs VALUES(?,?,?,?,?,?,NULL,0,NULL,0)',
                        (name, market, symbol, channel, self.now() + delay, interval))
        if channel == 'depthSnapshot':
            self.db.execute('UPDATE jobs SET priority=1 WHERE id=? AND request_id IS NULL', (name,))

    def capacity(self, market):
        # 配額可由 exchangeInfo 更新，所有 worker 每次派送都讀最新值。
        row = self.db.execute('SELECT capacity FROM limits WHERE market=?', (market,)).fetchone()
        return row[0] if row else 1200

    def available(self, scope, weight, cap, period, regular=False):
        # 同時檢查本地滑動視窗及交易所回報；倒退時鐘採保守等待。
        now = self.now()
        self.db.execute('DELETE FROM usage WHERE scope=? AND at<?', (scope, now - period))
        remote = self.db.execute('SELECT until,used FROM cooldown WHERE scope=?', (scope,)).fetchone()
        if remote and remote[0] > now and remote[1] == -1: return False
        used = self.db.execute('SELECT coalesce(sum(weight),0) FROM usage WHERE scope=?', (scope,)).fetchone()[0]
        if remote and remote[0] > now: used = max(used, remote[1])
        if used + weight > cap: return False
        if regular:
            regular_used = self.db.execute('SELECT coalesce(sum(weight),0) FROM usage WHERE scope=? AND regular=1', (scope,)).fetchone()[0]
            if regular_used + weight > cap * 7 / 9: return False
        self.db.execute('INSERT INTO usage VALUES(?,?,?,?)', (scope, now, weight, int(regular)))
        return True

    def connection(self, market):
        # 連線嘗試預算跨 A/B/C 共用，避免同時重連突破 IP 配額。
        self.db.execute('BEGIN IMMEDIATE')
        try:
            limits = [(300, self.config['redundancy']['ws_attempts_per_5min']), *adapter(market).connection_limits]
            result = all(self.available(f'ws:{self.config["family"]}:{period}:{cap}', 1, cap, period) for period, cap in limits)
            if result: self.db.commit()
            else: self.db.rollback()
            return result
        except BaseException:
            self.db.rollback(); raise

    def claim(self, owner, ready=None):
        # 每次派送才扣權重；逾時租約保守重試，不退回已消耗預算。
        now = self.now()
        if not self.ready_jobs(now): return None
        self.db.execute('BEGIN IMMEDIATE')
        try:
            active = self.db.execute('SELECT count(*) FROM jobs WHERE lease>?', (now,)).fetchone()[0]
            rows = [] if active >= self.config['redundancy']['rest_concurrency'] else self.ready_jobs(now)
            for name, market, symbol, channel, interval, priority in rows:
                if ready and not ready(market, symbol, channel): continue
                if not self.reserve(market, channel, priority): continue
                request = uuid.uuid4().hex
                self.db.execute('UPDATE jobs SET owner=?,lease=?,request_id=? WHERE id=?',
                                (owner, now + self.config['rest_timeout'] + 15, request, name))
                self.db.commit()
                return dict(id=name, market=market, symbol=symbol, channel=channel, interval=interval, request_id=request)
            self.db.commit(); return None
        except BaseException:
            self.db.rollback(); raise

    def ready_jobs(self, now):
        # 各優先佇列都取一批，避免五百個等配額的快照擋住較輕的 OI。
        rows = []
        for priority in (0, 1):
            rows.extend(self.db.execute('SELECT id,market,symbol,channel,interval,priority FROM jobs WHERE due<=? AND lease<=? AND priority=? ORDER BY due LIMIT 100', (now, now, priority)).fetchall())
        return rows

    def reserve(self, market, channel, priority):
        # 所有限制視窗須一次通過；回滾失敗嘗試，避免部分扣額造成假壅塞。
        cost = (50 if market == 'binance' else 20) if channel == 'depthSnapshot' else 1
        if channel == 'exchangeInfo' and market == 'binance': cost = 20
        self.db.execute('SAVEPOINT quota')
        if priority:
            used = self.db.execute('SELECT coalesce(sum(weight),0) FROM usage WHERE scope=? AND regular=0 AND at>=?',
                                   ('rest:' + market, self.now() - 60)).fetchone()[0]
            if used + cost > self.capacity(market) * .2:
                self.db.execute('RELEASE quota'); return False
        windows = [('rest:' + market, cost, self.capacity(market) * .9, 60)]
        for kind, period, capacity in self.db.execute('SELECT kind,period,capacity FROM rate_windows WHERE market=?', (market,)):
            windows.append((f'{market}:{kind}:{period}', cost if kind == 'REQUEST_WEIGHT' else 1, capacity * .9, period))
        passed = all(self.available(scope, weight, cap, period, not priority) for scope, weight, cap, period in windows)
        if not passed: self.db.execute('ROLLBACK TO quota')
        self.db.execute('RELEASE quota'); return passed

    def finish(self, owner, job, success, cooldown=0, headers=None):
        # 輪詢只排下一輪，不追趕重疊請求；過期 owner 不能釋放新租約。
        now = self.now(); market = job['market']
        with self.db:
            owned = self.db.execute('SELECT 1 FROM jobs WHERE id=? AND owner=? AND request_id=?',
                                    (job['id'], owner, job['request_id'])).fetchone()
            if cooldown:
                self.db.execute('INSERT INTO cooldown VALUES(?,?,-1) ON CONFLICT(scope) DO UPDATE SET until=max(until,excluded.until),used=-1',
                                ('rest:' + market, now + cooldown))
            elif headers:
                value = {k.lower(): v for k, v in headers.items()}.get('x-mbx-used-weight-1m')
                if value is not None:
                    self.db.execute('INSERT INTO cooldown VALUES(?,?,?) ON CONFLICT(scope) DO UPDATE SET used=CASE WHEN until=excluded.until THEN max(used,excluded.used) ELSE excluded.used END,until=excluded.until',
                                    ('rest:' + market, (int(now) // 60 + 1) * 60, int(value)))
            # 過期租約的回應仍計入 IP 限流，但不得完成別人的新任務。
            if not owned: return
            if success: self.db.execute('INSERT OR REPLACE INTO progress VALUES(?,?)', (job['id'], now))
            interval = job['interval'] if success else max(5, cooldown)
            self.db.execute('UPDATE jobs SET due=?,owner=NULL,lease=0,priority=CASE WHEN ? THEN 0 ELSE priority END WHERE id=? AND owner=? AND request_id=?',
                            (now + interval, success, job['id'], owner, job['request_id']))

    def metadata(self, market, data):
        # 封存交易所限額及商品清單；未上市商品明確拒絕，不用空資料冒充成功。
        valid = {x['symbol'] for x in data['symbols']}
        missing = set(self.config['market_settings'][market]['symbols']) - valid
        if missing: raise ValueError(f'unknown {market} symbols: {sorted(missing)}')
        limits = [int(r['limit']) / int(r['intervalNum']) for r in data['rateLimits']
                  if r['rateLimitType'] == 'REQUEST_WEIGHT' and r['interval'] == 'MINUTE']
        if not limits: raise ValueError('exchangeInfo missing minute weight limit')
        cap = min(int(min(limits)), self.config['rest_weight_per_minute'].get(market, 1200))
        with self.db:
            self.db.execute('UPDATE limits SET capacity=?,verified=1,raw=? WHERE market=?', (cap, stable_json(data), market))
            units = {'SECOND': 1, 'MINUTE': 60, 'HOUR': 3600, 'DAY': 86400}
            for rule in data['rateLimits']:
                if rule['rateLimitType'] in ('REQUEST_WEIGHT', 'RAW_REQUESTS') and rule['interval'] in units:
                    self.db.execute('INSERT OR REPLACE INTO rate_windows VALUES(?,?,?,?)',
                                    (market, rule['rateLimitType'], units[rule['interval']] * int(rule['intervalNum']), int(rule['limit'])))
            for channel in ('openInterest', 'depthSnapshot'):
                self.db.execute('UPDATE jobs SET interval=? WHERE market=? AND channel=?', (self.interval(market, channel), market, channel))

    def request_snapshot(self, market, symbol):
        # 只有合併來源仍有缺口才提高快照優先級，worker 重連不觸發全量快照。
        with self.db:
            self.db.execute('UPDATE jobs SET due=min(due,?),priority=1 WHERE market=? AND symbol=? AND channel=?',
                            (self.now(), market, symbol, 'depthSnapshot'))

    def capacity_summary(self):
        # 主管讀取實際生效週期，讓容量不足的降頻在唯一運行日誌中可見。
        query = '''SELECT market,channel,max(interval),count(*) FROM jobs
            WHERE channel IN ('openInterest','depthSnapshot') GROUP BY market,channel'''
        result = []
        for market, channel, period, count in self.db.execute(query):
            remaining = self.db.execute('SELECT count(*) FROM jobs j LEFT JOIN progress p ON j.id=p.id WHERE market=? AND channel=? AND p.id IS NULL', (market, channel)).fetchone()[0]
            cost = (50 if market == 'binance' else 20) if channel == 'depthSnapshot' else 1
            per_minute = int(self.capacity(market) * .2 // cost) if channel == 'depthSnapshot' else None
            result.append(dict(market=market, channel=channel, interval=period, symbols=count,
                               capacity=self.capacity(market), warming_symbols=remaining,
                               initial_per_minute=per_minute, warming_budget_windows=math.ceil(remaining / per_minute) if per_minute else None))
        return result

    def endpoint(self, group, excluded=()):
        # 優先已驗證路由，失敗路由冷卻；不因重連自動跳到只有 ACK 的路由。
        rows = {r[0]: r[1:] for r in self.db.execute('SELECT endpoint,good,cooldown FROM routes WHERE id=?', (group['id'],))}
        usable = [u for u in group['urls'] if u not in excluded and rows.get(u, (0, 0))[1] <= time.time()]
        if not usable: return None
        return max(usable, key=lambda u: rows.get(u, (0, 0))[0])

    def route_result(self, group, endpoint, success):
        # 只有匹配行情可以提高路由可信度；有資料後的偶發斷線不立即換端點。
        with self.db:
            if success:
                self.db.execute('INSERT INTO routes VALUES(?,?,1,0) ON CONFLICT(id,endpoint) DO UPDATE SET good=1,cooldown=0', (group['id'], endpoint))
            else:
                self.db.execute('INSERT INTO routes VALUES(?,?,0,?) ON CONFLICT(id,endpoint) DO UPDATE SET good=0,cooldown=excluded.cooldown',
                                (group['id'], endpoint, time.time() + 60))

    def close(self):
        # 每個 worker 使用自己的連線，結束只關閉本地 handle。
        with self.lock: self.db.close()
