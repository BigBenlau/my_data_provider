"""每家交易所一個 supervisor：驗證 worker、可靠提交、健康觀察及滾動換代。"""
import asyncio
import fcntl
import json
from pathlib import Path
import shutil
import signal
import time
import uuid
from common import LOG, atomic_json, configure_logging, close_log
from .delivery import MergeStore, database, read_packet, write_packet
from .redundancy import group_config, identity, local_paths
from .replica_monitor import Monitor
from .rest_schedule import Schedule
from .container_runtime import Runtime
from .transport import adapter
from .retention import compress_ready


class Supervisor:
    """所有市場共用管理器；交易所差異只由設定與 adapter 提供。"""
    def __init__(self, root, config, runtime):
        # 同一 run 可接續；禁止把不同採集設定混入既有正式資料。
        self.root = Path(root); self.config = config; self.runtime = runtime
        self.paths = local_paths(root); self.options = config['redundancy']
        self.expected = identity(config, runtime.image); self.lock = asyncio.Lock()
        manifest_path = self.root / 'manifest.json'
        if manifest_path.exists() and json.loads(manifest_path.read_text()).get('identity') != self.expected:
            raise ValueError('existing run version/config/image mismatch; use new output')
        self.store = MergeStore(root, config); self.monitor = Monitor(self.store, config)
        self.schedule = Schedule(self.paths['control'] / 'schedule.sqlite', config)
        self.stop = asyncio.Event(); self.repair = {}; self.clients = set(); self.connections = set()
        self.fatal = None; self.last_health = 0; self.compression = None; self.last_compression = 0
        self.capacity_state = None; self.last_archive = 0
        self.state_path = self.paths['control'] / 'supervisor.json'
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {
            'active': [], 'candidate': None, 'retiring': None, 'last_rotation': time.time(), 'next_index': 0}
        self.manifest = self.prepare_manifest()

    def prepare_manifest(self):
        # 正式 JSONL 使用第三版；舊資料仍由既有 reader 支援。
        path = self.root / 'manifest.json'
        if path.exists():
            manifest = json.loads(path.read_text())
            if manifest.get('identity') != self.expected: raise ValueError('existing run version/config/image mismatch; use new output')
        else:
            manifest = dict(schema_version=3, run_id=self.root.name, started_us=time.time_ns() // 1000,
                            config=self.config, identity=self.expected, channel_csv_schema_version=1,
                            processing='replica_merge_only', order_book_reconstruction=False,
                            outputs=['raw_jsonl'], channel_csv_export='on_demand')
        manifest['status'] = 'running'; atomic_json(path, manifest)
        return manifest

    def save(self):
        # 先記錄角色再執行容器動作，崩潰後可以依相同 worker 名稱繼續。
        atomic_json(self.state_path, self.state)

    def workers(self):
        # retiring 仍計入三個副本上限，排空未完成時不建立另一候選者。
        ids = list(self.state['active'])
        for role in ('candidate', 'retiring'):
            if self.state[role]: ids.append(self.state[role]['worker'])
        return sorted(set(ids))

    async def locked(self, function, *args):
        # SQLite 合併寫入序列化，但 fsync 放入背景執行緒，不阻塞 IPC 收訊。
        async with self.lock: return await asyncio.to_thread(function, *args)

    async def accept(self, reader, writer):
        # 身分不合格不接受資料；只有目前管理的 worker 可連入。
        task = asyncio.current_task(); self.clients.add(task); connection = None; registered = False
        try:
            hello = await asyncio.wait_for(read_packet(reader), 10); worker = hello.get('worker')
            role = hello.get('role', 'data'); connection = (worker, role)
            if hello.get('type') != 'hello' or worker not in self.workers() or hello.get('identity') != self.expected:
                await write_packet(writer, {'error': 'worker version/config/image rejected'}); return
            if role not in ('data', 'control') or connection in self.connections or not await self.runtime.alive(worker):
                await write_packet(writer, {'error': 'worker process unavailable or duplicate connection', 'retry': True}); return
            self.connections.add(connection); registered = True; await write_packet(writer, {'accepted': True})
            while True:
                request = await read_packet(reader)
                if worker not in self.workers(): break
                if request.get('type') == 'batch' and role == 'data':
                    ack = await self.locked(self.store.ingest, worker, request['batch'])
                    await write_packet(writer, {'ack': ack})
                elif request.get('type') == 'status' and role == 'control':
                    self.monitor.status(worker, request['status'])
                    await write_packet(writer, {'repair': self.repair.pop(worker, [])})
                else: raise ValueError('unknown IPC request')
        except (asyncio.IncompleteReadError, ConnectionError, BrokenPipeError): pass
        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError):
                LOG.exception('event=supervisor_ingest_failed worker=%s', worker); self.fatal = exc; self.stop.set()
        finally:
            if registered: self.connections.discard(connection)
            self.clients.discard(task); writer.close()
            try: await writer.wait_closed()
            except (OSError, ConnectionError): pass

    async def start_missing(self):
        # 崩潰的 worker 先用相同身分恢復 spool；不刪除未 ACK 暫存。
        while len(self.state['active']) < 2:
            worker = uuid.uuid4().hex[:12]; self.state['active'].append(worker); self.save()
        for worker in self.state['active']:
            if not await self.runtime.alive(worker): await self.runtime.start(worker)
        candidate = self.state['candidate']
        if candidate and not await self.runtime.alive(candidate['worker']):
            await self.runtime.start(candidate['worker'])

    async def rotation(self):
        # 延期不追趕；候選失敗亦等待下一個週期，避免重連耗盡配額。
        if self.state['retiring']:
            await self.finish_retiring(); return
        candidate = self.state['candidate']; now = time.time()
        if candidate:
            valid, reasons = await self.locked(self.monitor.verify, candidate['worker'], self.state['active'], candidate['since'])
            if valid:
                index = self.state['next_index'] % 2; old = self.state['active'][index]
                self.state['active'][index] = candidate['worker']; self.state['next_index'] = 1 - index
                self.state['candidate'] = None; self.retire(old, 'verified_replacement')
            elif now - candidate['since'] >= self.options['candidate_timeout']:
                LOG.warning('event=candidate_rejected worker=%s reasons=%s', candidate['worker'], json.dumps(reasons))
                self.state['candidate'] = None; self.retire(candidate['worker'], 'verification_failed')
            return
        if now - self.state['last_rotation'] < self.options['rotation_seconds']: return
        if self.monitor.blockers:
            LOG.warning('event=rotation_blocked policy=strict reasons=%s action=retain_workers', json.dumps(self.monitor.blockers))
            self.state['last_rotation'] = now; self.save(); return
        worker = uuid.uuid4().hex[:12]
        self.state['candidate'] = dict(worker=worker, since=now); self.save()
        await self.runtime.start(worker)

    def retire(self, worker, reason):
        # 指令只要求停止收訊，worker 必須先排空交付才自行退出。
        self.state['retiring'] = dict(worker=worker, since=time.time(), reason=reason)
        self.save(); (self.paths['control'] / (worker + '.drain')).touch()
        LOG.info('event=worker_drain_requested worker=%s reason=%s', worker, reason)

    async def finish_retiring(self):
        # 不以期限強殺尚未交付資料；逾時持續告警並暫停下一次換代。
        item = self.state['retiring']; worker = item['worker']
        (self.paths['control'] / (worker + '.drain')).touch()
        if not await self.runtime.alive(worker):
            await self.locked(self.recover_spool, worker)
            await self.runtime.stop(worker)
            await self.locked(self.monitor.forget, worker)
            self.state['retiring'] = None; self.state['last_rotation'] = time.time(); self.save()
            LOG.info('event=worker_retired worker=%s reason=%s', worker, item['reason'])
        elif time.time() - item['since'] > self.options['drain_timeout']:
            LOG.warning('event=worker_drain_pending worker=%s action=retain_spool_and_pause_rotation', worker)

    def recover_spool(self, worker):
        # 僅接管已退出程序的待交付資料；正式提交成功後才移除該副本暫存。
        root = self.paths['spool'] / worker; path = root / 'spool.sqlite'
        if not path.exists(): return
        db = database(path)
        try:
            while True:
                rows = db.execute('SELECT seq,payload FROM items ORDER BY seq LIMIT 512').fetchall()
                if not rows: break
                ack = self.store.ingest(worker, [{'seq': n, 'record': json.loads(p)} for n, p in rows])
                with db:
                    db.execute('DELETE FROM items WHERE seq<=?', (ack,)); db.execute('UPDATE state SET ack=?', (ack,))
            db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        finally: db.close()
        shutil.rmtree(root)

    async def maintain_raw(self, now):
        # 壓縮在獨立執行緒讀封段，慢磁碟不會佔住交付鎖；失敗保留原文重試。
        if self.compression and self.compression.done():
            try:
                self.compression.result()
            except Exception:
                LOG.exception('event=raw_compression_failed action=retry')
            self.compression = None
        if self.options['compress_raw'] and not self.compression and now - self.last_compression >= 5:
            self.compression = asyncio.create_task(asyncio.to_thread(compress_ready, self.root))
            self.last_compression = now
        if now - self.last_archive >= self.options['archive_seconds']:
            started = time.monotonic()
            moved = await self.locked(self.store.archive.move, self.options['index_hot_seconds'], self.options['archive_batch_size'])
            self.last_archive = time.monotonic()
            elapsed = self.last_archive - started
            if moved or elapsed > .25:
                LOG.info('event=index_archive_batch records=%s elapsed_ms=%.1f rows_per_s=%.1f',
                         moved, elapsed * 1000, moved / max(elapsed, .000001))

    async def background(self):
        # 心跳列出可匯出的封段上限；匯出程序自行記錄其所在目的地的游標。
        now = time.monotonic()
        await self.maintain_raw(now)
        if now - self.last_health >= self.options['heartbeat_seconds']:
            await asyncio.to_thread(self.capacity_log)
            self.repair, gaps = await self.locked(self.monitor.health, self.workers(), self.state['active']); self.last_health = now
            for gap, feed in await self.locked(self.monitor.snapshot_requests):
                market, symbol, _ = feed.split('/'); await self.schedule.call('request_snapshot', market, symbol)
                await self.locked(self.monitor.snapshot_requested, gap)
            report = dict(self.monitor.latest, containers={w: await self.runtime.alive(w) for w in self.workers()})
            atomic_json(self.root / 'status.json', report)
            sealed = await self.locked(self.store.sealed_cursor)
            LOG.info('event=output_health jsonl_seq=%s sealed_seq=%s csv_mode=on_demand', report['raw_records'], sealed)

    def capacity_log(self):
        # 只有配額或週期改變時告警；worker 換代不重複製造相同降頻訊息。
        with self.schedule.lock: state = self.schedule.capacity_summary()
        if state == self.capacity_state: return
        self.capacity_state = state
        for row in state:
            LOG.info('event=source_warmup market=%s channel=%s remaining=%s initial_per_minute=%s budget_windows=%s',
                     row['market'], row['channel'], row['warming_symbols'], row['initial_per_minute'], row['warming_budget_windows'])
            requested = adapter(row['market']).rest_policy.requested_interval(self.config, row['channel'])
            if row['interval'] > requested:
                LOG.warning('event=rest_capacity_adjusted market=%s channel=%s symbols=%s requested_s=%s effective_s=%s budget_per_min=%s',
                            row['market'], row['channel'], row['symbols'], requested, row['interval'], row['capacity'])
            else: LOG.info('event=rest_capacity market=%s channel=%s interval_s=%s symbols=%s', row['market'], row['channel'], row['interval'], row['symbols'])

    async def serve(self, duration=None):
        # 重啟 supervisor 只重建 socket；既有 worker 自動重送與追趕。
        self.paths['socket'].unlink(missing_ok=True)
        server = await asyncio.start_unix_server(self.accept, path=str(self.paths['socket']))
        started = time.monotonic()
        if self.monitor.blockers:
            LOG.warning('event=rotation_preflight policy=strict blocked=%s action=retain_workers', json.dumps(self.monitor.blockers))
        try:
            while not self.stop.is_set():
                await self.start_missing(); await self.background(); await self.rotation()
                if duration and time.monotonic() - started >= duration: break
                try: await asyncio.wait_for(self.stop.wait(), .5)
                except TimeoutError: pass
            if self.fatal: raise self.fatal
            await self.shutdown()
        finally:
            # Python 3.12 會等現有 socket 關閉；先取消處理器，避免 wait_closed 互等。
            server.close(); clients = list(self.clients)
            for task in clients: task.cancel()
            await asyncio.gather(*clients, return_exceptions=True)
            await server.wait_closed()
            if self.compression: await asyncio.gather(self.compression, return_exceptions=True)

    async def shutdown(self):
        # 使用者正常停止時排空所有副本；程序崩潰不會走此路徑。
        workers = self.workers()
        for worker in workers: (self.paths['control'] / (worker + '.drain')).touch()
        deadline = time.monotonic() + self.options['drain_timeout'] + 5
        last_report = 0
        while any([await self.runtime.alive(w) for w in workers]) and time.monotonic() < deadline:
            if time.monotonic() - last_report >= self.options['heartbeat_seconds']:
                await self.locked(self.monitor.health, workers, self.state['active']); last_report = time.monotonic()
                LOG.info('event=shutdown_progress phase=draining_workers')
            await asyncio.sleep(.2)
        for worker in workers:
            await self.runtime.stop(worker)
            await self.locked(self.recover_spool, worker)
            await self.locked(self.monitor.forget, worker)
        self.state.update(active=[], candidate=None, retiring=None, last_rotation=time.time()); self.save()
        await self.locked(self.store.seal)
        if self.compression:
            await asyncio.gather(self.compression, return_exceptions=True)
            self.compression = None
        if self.options['compress_raw']:
            while await asyncio.to_thread(compress_ready, self.root):
                pass
        await self.locked(self.monitor.health, [])
        await self.locked(self.monitor.stopped)
        atomic_json(self.root / 'status.json', self.monitor.latest)
        self.manifest.update(status='stopped', ended_us=time.time_ns() // 1000,
                             raw_records=self.monitor.latest['raw_records'], health_at_stop=self.monitor.latest)
        atomic_json(self.root / 'manifest.json', self.manifest)


async def run_supervisor(args):
    # 單機檔案鎖排除重複主管；正式容器不依賴 Docker Compose 的 worker 副本數。
    root = Path(args.output).resolve(); root.mkdir(parents=True, exist_ok=True)
    lock = (root / '.collector.lock').open('a'); runtime = supervisor = file_log = None
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        config = group_config(args.exchange, args.config)
        control = root / '_state'; control.mkdir(exist_ok=True)
        path = control / 'config.json'
        if path.exists() and json.loads(path.read_text()) != config: raise ValueError('existing run configuration differs')
        atomic_json(path, config)
        file_log = configure_logging(root / 'supervisor.log', config['log_max_bytes'], config['log_backup_count'])
        runtime = Runtime(root, config, args.backend, args.image, getattr(args, 'data_volume', None) or args.host_output)
        await runtime.open(); supervisor = Supervisor(root, config, runtime)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, supervisor.stop.set)
        await supervisor.serve(args.duration)
        return 0
    finally:
        try:
            if supervisor: supervisor.schedule.close(); supervisor.store.close()
        finally:
            if runtime: await runtime.close()
            close_log(file_log); lock.close()
