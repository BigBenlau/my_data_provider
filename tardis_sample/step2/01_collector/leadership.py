"""主管換版：候選唯讀預檢、單一寫入鎖、同一組收訊副本與可回退的交接。"""
import asyncio
import fcntl
import json
import os
import time
import uuid
from pathlib import Path
from common import LOG, atomic_json
from .redundancy import code_hashes, fingerprint, configured_feeds
from .retention import sync_directory
from .transport import key


def read(path):
    # 缺檔表示狀態尚未建立；損壞的決策檔必須明確報錯。
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return {}


def publish(path, value):
    # 決策與序號必須同樣耐久，確保主機重啟後只有一位正式寫入者。
    atomic_json(path, value)
    sync_directory(path.parent)


def acquire(path):
    # 鎖檔永不刪除；程序崩潰時核心會釋放其持有的 flock。
    handle = path.open('a')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    return handle


def busy(path):
    # 只探測鎖的持有者，不以 PID 或時間戳推斷寫入權。
    handle = acquire(path)
    if handle is None:
        return True
    handle.close()
    return False


def compatibility():
    # 主管管理程式可獨立更新；原文、協議、儲存與 worker 的程式須相容。
    management = {'01_collect.py', 'supervisor.py', 'container_runtime.py', 'replica_monitor.py', 'leadership.py'}
    return fingerprint({p: h for p, h in code_hashes().items() if Path(p).name not in management})


def evidence(report, feeds, max_age):
    # 使用當期兩路會話、已確認位置及收訊計數，拒絕只靠容器存活或舊心跳。
    now = time.time()
    active = report.get('active', [])
    if (len(set(active)) != 2 or report.get('commit_blocked') or report.get('storage_blocked_workers') or report.get('uncovered_gaps')
            or not 0 <= now - report.get('updated_us', 0) / 1e6 <= max_age):
        return None
    paths = {}
    for worker in active:
        state = report.get('workers', {}).get(worker, {})
        if (state.get('draining') or not report.get('containers', {}).get(worker)
                or not 0 <= now - state.get('reported_at', 0) <= max_age):
            return None
        sessions = {}
        received = 0
        for name in feeds:
            source = state.get('feeds', {}).get(name, {})
            if not (source.get('session') and source.get('subscribed') and source.get('connected')):
                return None
            sessions[name] = source['session']
            received += source.get('received', 0)
        if not feeds:
            # 純 REST 批次共用任務租約，任一副本都可能合法地沒有領到請求。
            received = sum(v.get('received', 0) for v in state.get('feeds', {}).values())
        paths[worker] = dict(started=state['started'], sessions=sessions, received=received, ack=state['ack'])
    return dict(paths=paths, seq=report['raw_records'], at=report['updated_us'] / 1e6)


def progressed(first, last, seconds):
    # 收訊不間斷須沿用每條 WS 會話；兩個 worker 的實際行情及耐久 ACK 都須前進。
    if not first or not last or first['paths'].keys() != last['paths'].keys():
        return False
    if last['seq'] <= first['seq'] or last['at'] - first['at'] < seconds:
        return False
    acked = received = 0
    for worker, a in first['paths'].items():
        b = last['paths'][worker]
        ack_delta, data_delta = b['ack'] - a['ack'], b['received'] - a['received']
        if (a['started'] != b['started'] or a['sessions'] != b['sessions'] or min(ack_delta, data_delta) < 0
                or a['sessions'] and min(ack_delta, data_delta) == 0):
            return False
        acked += ack_delta
        received += data_delta
    return min(acked, received) > 0


class Leadership:
    """交接期間舊程序保留回退能力；正式 JSONL 始終只有一個寫入者。"""

    def __init__(self, root, args, config):
        # 所有角色在同一持久目錄交接，第二個候選無法同時發起換版。
        self.root = Path(root)
        self.control = self.root / '_state'
        self.writer = self.root / '.collector.lock'
        self.lease = self.control / 'upgrade.lock'
        self.owner = self.control / 'owner.json'
        self.request = self.control / 'upgrade-request.json'
        self.grant = self.control / 'upgrade-grant.json'
        self.name = args.supervisor_id
        self.config = config
        self.mount = str(args.data_volume or args.host_output or self.root)
        self.backend = args.backend
        self.code = fingerprint(code_hashes())
        self.contract = compatibility()
        self.feeds = [key(f) for f in configured_feeds(config)]
        options = config['redundancy']
        self.observe = max(10, options['verify_seconds'])
        self.age = options['worker_timeout']
        self.timeout = args.upgrade_timeout
        self.data_lock = self.writer
        lock_root = os.environ.get('SUPERVISOR_LOCK_ROOT')
        if lock_root:
            # 原生 Docker volume 跨主管共享檔鎖；依交易所及實際掛載隔離不同批次。
            lock_dir = Path(lock_root) / fingerprint([config['family'], self.mount])
            lock_dir.mkdir(parents=True, exist_ok=True)
            self.writer, self.lease = lock_dir / 'writer.lock', lock_dir / 'upgrade.lock'
        self.cancel = asyncio.Event()
        self.yielded = False
        self.trial = None

    def record(self, worker_identity, retired=()):
        # 將 supervisor 版本與整組 worker 的固定映像身分分別登記。
        return dict(protocol=1, owner=self.name, code=self.code, contract=self.contract,
                    backend=self.backend, mount=self.mount, external_lock=self.data_lock != self.writer, worker_identity=worker_identity, retired=list(retired))

    def check(self, owner):
        # 候選在要求舊主管讓出寫入權前核對設定、掛載與相容性。
        worker = owner['worker_identity']
        manifest = read(self.root / 'manifest.json')
        if (owner['protocol'] != 1 or owner['contract'] != self.contract
                or owner['mount'] != self.mount or owner['backend'] != self.backend
                or owner.get('external_lock', False) != (self.data_lock != self.writer)
                or worker['config_hash'] != self.config['config_hash'] or manifest.get('identity') != worker):
            raise ValueError('incompatible supervisor, data mount, run or worker contract')
        if self.backend == 'local' and self.code != worker['code_hash']:
            raise ValueError('cross-version supervisor upgrades require pinned Docker worker images')

    def sample(self, name):
        # 新舊心跳使用不同主管識別，過去的成功心跳不能用來證明候選就緒。
        report = read(self.root / 'status.json')
        if report.get('supervisor_id') != name:
            return None
        return evidence(report, self.feeds, self.age)

    async def observe_owner(self, owner, deadline):
        # 候選保持唯讀直到舊主管與原 A/B 已共同持續收訊至少十秒。
        first = None
        while not self.cancel.is_set() and time.monotonic() < deadline:
            sample = self.sample(owner['owner'])
            if progressed(first, sample, self.observe):
                return sample
            if not sample or not first or any(first['paths'].get(w, {}).get('sessions') != p['sessions']
                                                for w, p in sample['paths'].items()):
                first = sample
            await asyncio.sleep(.25)
        raise TimeoutError('incumbent capture continuity not verified')

    def tick(self, supervisor):
        # 寫入鎖只在關閉舊 socket 及資料庫後才轉移；正式換主還需新版雙路 ACK。
        if self.trial:
            return self.trial_tick(supervisor)
        if self.cancel.is_set():
            supervisor.stop.set()
            return False
        request = read(self.request)
        if (request.get('from') != self.name or request.get('until', 0) <= time.time()
                or supervisor.state['candidate'] or supervisor.state['retiring'] or supervisor.storage_holds()
                or not self.live_request(request)):
            return False
        proof = self.sample(self.name)
        if not proof or not self.same_sessions(request['proof'], proof):
            return False
        publish(self.grant, dict(token=request['token'], proof=proof))
        self.yielded = True
        LOG.info('event=supervisor_writer_yielded supervisor=%s candidate=%s', self.name, request['to'])
        return True

    def live_request(self, request):
        # 檔鎖及候選世代須同時存在；上次失敗留下的請求不能使主管再次讓位。
        return (request.get('token') and read(self.control / 'upgrade-intent.json').get('token') == request['token']
                and busy(self.lease))

    def grant_pending(self):
        # 即使舊主管在讓位期間重啟，也需讓持有同一授權的候選先取得寫入鎖。
        request = read(self.request)
        return (request.get('from') == self.name and request.get('until', 0) > time.time()
                and read(self.grant).get('token') == request.get('token')
                and self.live_request(request))

    @staticmethod
    def same_sessions(first, last):
        # 驗證兩組完整的 worker、程序起點及各商品的會話快照。
        a, b = first['paths'], last['paths']
        return a.keys() == b.keys() and all(a[w]['sessions'] == b[w]['sessions']
                                             and a[w]['started'] == b[w]['started'] for w in a)

    def trial_tick(self, supervisor):
        # 候選未證實兩路續傳時故障或逾時，只關閉 IPC，保留 worker 給原主管接回。
        trial = self.trial
        if self.cancel.is_set() or time.monotonic() >= trial['deadline']:
            LOG.error('event=supervisor_upgrade_rollback candidate=%s', self.name)
            return True
        proof = self.sample(self.name)
        if not progressed(trial['proof'], proof, 0):
            return False
        if not all((w, role) in supervisor.connections for w in proof['paths'] for role in ('control', 'data')):
            return False
        old = trial['owner']
        publish(self.owner, self.record(old['worker_identity'], [*old['retired'], old['owner']]))
        self.trial = None
        self.upgrade_lock.close()
        LOG.info('event=supervisor_promoted supervisor=%s previous=%s seq=%s', self.name, old['owner'], proof['seq'])
        return False

    async def candidate(self, owner, activate):
        # 候選與舊主管並行，未獲專屬授權與正式寫入鎖前不開啟可寫資料庫。
        lease = acquire(self.lease)
        if not lease:
            raise RuntimeError('another supervisor upgrade is in progress')
        self.upgrade_lock = lease
        try:
            token = uuid.uuid4().hex
            publish(self.control / 'upgrade-intent.json', dict(token=token))
            self.check(owner)
            if read(self.owner) != owner:
                raise RuntimeError('supervisor ownership changed')
            deadline = time.monotonic() + self.timeout
            proof = await self.observe_owner(owner, deadline)
            if not busy(self.writer):
                raise RuntimeError('incumbent writer lock is not visible; check shared lock volume')
            publish(self.request, {'from': owner['owner'], 'to': self.name, 'token': token,
                                   'until': time.time() + max(0, deadline-time.monotonic()), 'proof': proof})
            while not self.cancel.is_set() and time.monotonic() < deadline:
                grant = read(self.grant)
                if grant.get('token') == token:
                    handle = acquire(self.writer)
                    if handle:
                        with handle:
                            self.trial = dict(owner=owner, proof=grant['proof'], deadline=deadline)
                            return await activate(owner)
                await asyncio.sleep(.2)
            raise TimeoutError('candidate could not obtain writer grant')
        finally:
            # 候選若崩潰會由核心釋放；舊主管保留原所有權即可自動接回。
            lease.close()

    async def manage(self, takeover, activate):
        # 舊主管在正式升級後留在待命狀態；容器重啟也不能重複發起同一世代交接。
        while not self.cancel.is_set():
            owner = read(self.owner)
            if takeover and not owner:
                raise ValueError('incumbent does not support coordinated upgrade')
            if owner and owner['owner'] != self.name:
                if takeover and self.name not in owner['retired']:
                    LOG.info('event=supervisor_shadow_started candidate=%s incumbent=%s', self.name, owner['owner'])
                    result = await self.candidate(owner, activate)
                    if self.yielded:
                        continue
                    return result
                await asyncio.sleep(.5)
                continue
            if self.grant_pending():
                await asyncio.sleep(.2)
                continue
            handle = acquire(self.writer)
            if not handle:
                if self.yielded or read(self.request).get('from') == self.name:
                    await asyncio.sleep(.2)
                    continue
                raise RuntimeError('run already has an active writer')
            with handle:
                if owner != read(self.owner):
                    continue
                self.yielded = False
                result = await activate(owner)
                if not self.yielded:
                    return result
            LOG.info('event=supervisor_standby supervisor=%s', self.name)
        return 0
