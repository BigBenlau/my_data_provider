"""每家交易所一個 supervisor：驗證 worker、可靠提交、健康觀察及滾動換代。"""
import asyncio
import json
from pathlib import Path
import re
import shutil
import sqlite3
import math
import aiohttp
import signal
import time
import uuid
from common import LOG, atomic_json, configure_logging, close_log, utc
from .delivery import MergeStore, database, read_packet, write_packet
from .redundancy import group_config, identity, local_paths
from .replica_monitor import Monitor
from .ws_rotation import Rotation
from .rest_schedule import Schedule
from .container_runtime import Runtime, WorkerFailure, RuntimeUnavailable
from .transport import adapter
from .retention import compress_ready
from .cold import archive_files
from .storage_health import StorageHealth
from .raw import StorageError
from .leadership import Leadership, acquire, publish, read


def new_run_id():
    # UTC 時戳便於按開始時間排序，隨機後綴用來區分同秒建立的批次。
    return utc(time.time_ns() // 1000, '%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]


def initial_run_id(root, family, runtime):
    # Compose 的 /data 看不到主機批次名稱，使用實際掛載路徑辨識自動建立的批次。
    mount = Path(getattr(runtime, 'host_root', None) or root)
    batch_id = mount.parent.name
    if mount.name == family and re.fullmatch(r'[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}', batch_id):
        return batch_id
    return new_run_id()


class Supervisor:
    """所有市場共用管理器；交易所差異只由設定與 adapter 提供。"""
    def __init__(self, root, config, runtime, expected=None):
        # 同一 run 可接續；禁止把不同採集設定混入既有正式資料。
        self.root = Path(root); self.config = config; self.runtime = runtime
        self.paths = local_paths(root); self.options = config['redundancy']
        self.expected = expected or identity(config, runtime.image); self.lock = asyncio.Lock()
        manifest_path = self.root / 'manifest.json'
        if manifest_path.exists() and (json.loads(manifest_path.read_text()).get('dedup_rule_version') != 2 or
                                       json.loads(manifest_path.read_text()).get('identity') != self.expected):
            raise ValueError('existing run version/config/image mismatch; use new output')
        self.store = MergeStore(root, config, background_notices=True); self.monitor = Monitor(self.store, config)
        self.schedule = Schedule(self.paths['control'] / 'schedule.sqlite', config)
        self.stop = asyncio.Event(); self.repair = {}; self.clients = set(); self.connections = set()
        self.output_fault = None; self.output_retry = 0
        self.last_health = 0; self.compression = None; self.last_compression = 0
        self.capacity_state = None; self.last_archive = 0
        self.recovery_positions = {}
        self.recovery_dbs = {}; self.retry_at = {}; self.detach = False; self.unsaved_state = False
        self.on_tick = None; self.in_trial = lambda: False; self.supervisor_id = "primary"
        self.state_path = self.paths['control'] / 'supervisor.json'
        self.state = json.loads(self.state_path.read_text()) if self.state_path.exists() else {
            'active': [], 'candidate': None, 'retiring': None, 'last_rotation': time.time(), 'next_index': 0}
        self.ws_rotation = Rotation(self)
        self.disks = StorageHealth(self.root)
        self.transfer = None; self.cold_retry = 0
        self.manifest = self.prepare_manifest()

    def prepare_manifest(self):
        # 正式 JSONL 使用第三版；舊資料仍由既有 reader 支援。
        path = self.root / 'manifest.json'
        if path.exists():
            # 重啟與交接沿用已保存的 run_id，讓批次及匯出來源身分保持一致。
            manifest = json.loads(path.read_text())
            if manifest.get('identity') != self.expected: raise ValueError('existing run version/config/image mismatch; use new output')
        else:
            run_id = initial_run_id(self.root, self.config['family'], self.runtime)
            manifest = dict(schema_version=3, dedup_rule_version=2, run_id=run_id, started_us=time.time_ns() // 1000,
                            config=self.config, identity=self.expected, channel_csv_schema_version=1,
                            processing='replica_merge_only', order_book_reconstruction=False,
                            outputs=['raw_jsonl'], raw_partition='utc_receive_date', channel_csv_export='on_demand')
        manifest['status'] = 'running'; atomic_json(path, manifest)
        return manifest

    def save(self):
        # 先記錄角色再執行容器動作，崩潰後可以依相同 worker 名稱繼續。
        self.unsaved_state = True
        publish(self.state_path, self.state)
        self.unsaved_state = False

    def workers(self):
        # retiring 仍計入三個副本上限，排空未完成時不建立另一候選者。
        ids = list(self.state['active'])
        for role in ('candidate', 'retiring'):
            if self.state[role]: ids.append(self.state[role]['worker'])
        return sorted(set(ids))

    async def locked(self, function, *args):
        # 協程取消不能中斷背景 SQLite 交易，等交易結束才釋放鎖給下一條交付連線。
        async with self.lock:
            operation = asyncio.create_task(asyncio.to_thread(function, *args))
            cancelled = False
            while not operation.done():
                try:
                    await asyncio.shield(operation)
                except asyncio.CancelledError:
                    cancelled = True
            result = operation.result()
            if cancelled:
                raise asyncio.CancelledError
            return result

    def storage_holds(self):
        # 未完成補交的事故持續列在狀態檔；已恢復的舊格式事故仍可讀取。
        return {w for w, item in self.state.get('storage_incidents', {}).items()
                if 'resumed_us' not in item and 'recovered_us' not in item}

    def hold_storage(self, worker, exc):
        # 事故與替補意圖先持久化，舊身分永遠不能再佔用新的收訊席位。
        if worker in self.storage_holds():
            return
        incidents = self.state.setdefault('storage_incidents', {})
        try:
            failure = read(self.paths['control'] / (worker + '.failure.json'))
        except (OSError, ValueError) as error:
            failure = dict(report_error=str(error))
        exit_state = getattr(self.runtime, 'exit_reasons', {}).get(worker, {})
        incidents[worker] = dict(detected_us=time.time_ns() // 1000, reason=str(exc), worker_error=failure, exit_state=exit_state)
        self.save()
        LOG.error('event=worker_quarantined worker=%s reason=%s action=replace_and_recover worker_error=%s exit_state=%s', worker, exc, failure, exit_state)

    def retry(self, worker, exc):
        # Docker 失聯表示狀態未知，不能誤判程序死亡並反覆建立容器。
        self.retry_at[worker] = time.monotonic() + 5
        LOG.warning('event=worker_retry worker=%s reason=%s retry_s=5', worker, exc)

    async def start_worker(self, worker):
        # 個別副本的啟動錯誤只影響該席位；其餘路徑繼續接收與提交。
        try:
            await self.runtime.start(worker)
        except (WorkerFailure, ValueError) as exc:
            self.hold_storage(worker, exc)
        except (aiohttp.ClientError, RuntimeUnavailable, OSError, TimeoutError) as exc:
            self.retry(worker, exc)

    async def worker_alive(self, worker):
        # 區分已死、存活與狀態未知，不因 Docker 暫時無回應而關停主管。
        if (self.paths['control'] / (worker + '.failure.json')).exists() and worker not in self.storage_holds():
            self.hold_storage(worker, 'worker reported a fatal exception')
        if worker in self.storage_holds() or self.retry_at.get(worker, 0) > time.monotonic():
            return None
        try:
            return await self.runtime.alive(worker)
        except (WorkerFailure, ValueError) as exc:
            self.hold_storage(worker, exc)
        except (aiohttp.ClientError, RuntimeUnavailable, OSError, TimeoutError) as exc:
            self.retry(worker, exc)
        return None

    def replace_role(self, worker, incident):
        # 同一席位第一次立即替補；連續故障逐步退避，避免共用磁碟故障耗盡容器資源。
        if worker in self.state['active']:
            slot = self.state['active'].index(worker)
            new = uuid.uuid4().hex[:12]
            previous = self.state.setdefault('slot_failures', {}).get(str(slot), {})
            count = previous.get('count', 0) + 1 if time.time() - previous.get('at', 0) < 120 else 1
            delay = 0 if count == 1 else min(60, 2 ** min(6, count - 1))
            self.state['slot_failures'][str(slot)] = dict(count=count, at=time.time())
            self.state['active'][slot] = new
            incident.update(replacement=new, start_after=time.time()+delay)
            LOG.warning('event=worker_replaced worker=%s replacement=%s slot=%s delay_s=%s', worker, new, slot, delay)
        for role in ('candidate', 'retiring'):
            if self.state[role] and self.state[role]['worker'] == worker:
                self.state[role] = None
        incident['isolated'] = True
        self.save()

    async def repair_failed(self):
        # 先停止舊程序，再換新席位；完整持久批次分批補交，損壞資料保留供查驗。
        for worker in sorted(self.storage_holds()):
            if self.retry_at.get(worker, 0) > time.monotonic():
                continue
            incident = self.state['storage_incidents'][worker]
            try:
                if not incident.get('isolated'):
                    await self.runtime.quiesce(worker)
                    self.replace_role(worker, incident)
                    continue
                if not await self.locked(self.recover_spool_batch, worker):
                    continue
                await self.runtime.stop(worker)
                await self.locked(self.monitor.forget, worker)
                incident['recovered_us'] = time.time_ns() // 1000
                self.save()
                LOG.info('event=worker_spool_recovered worker=%s', worker)
            except Exception as exc:
                self.retry(worker, exc)

    def storage_report(self, report):
        # 來源缺失的品質判定優先；曾發生的儲存事故要留待核對。
        report['ws_rotation'] = self.ws_rotation.s
        report['storage'] = dict(self.disks.latest)
        if self.store.cold.error:
            report['storage']['cold_error'] = self.store.cold.error
        incidents = self.state.get('storage_incidents', {})
        if incidents:
            report.update(storage_incidents=incidents, storage_blocked_workers=sorted(self.storage_holds()))
            if report.get('data_quality') != 'incomplete':
                report['data_quality'] = 'coverage_unverified_after_storage_failure'
        return report

    async def accept(self, reader, writer):
        # 身分不合格不接受資料；只有目前管理的 worker 可連入。
        task = asyncio.current_task(); self.clients.add(task); connection = None; registered = False; worker = None
        try:
            hello = await asyncio.wait_for(read_packet(reader), 10); worker = hello.get('worker')
            role = hello.get('role', 'data'); connection = (worker, role)
            if hello.get('type') != 'hello' or worker not in self.workers() or hello.get('identity') != self.expected:
                await write_packet(writer, {'error': 'worker version/config/image rejected'}); return
            if role not in ('data', 'control') or connection in self.connections or not await self.worker_alive(worker):
                await write_packet(writer, {'error': 'worker process unavailable or duplicate connection', 'retry': True}); return
            self.connections.add(connection); registered = True; await write_packet(writer, {'accepted': True})
            while True:
                request = await read_packet(reader)
                if not isinstance(request, dict): raise ValueError('invalid IPC request')
                if worker not in self.workers() or worker in self.storage_holds(): break
                if request.get('type') == 'batch' and role == 'data':
                    batch = request['batch']
                    if not isinstance(batch, list) or not batch or any(
                            not isinstance(i, dict) or type(i.get('seq')) is not int or i['seq'] <= 0 or
                            not isinstance(i.get('record'), dict) for i in batch):
                        raise ValueError('invalid worker batch')
                    ack = await self.locked(self.commit_batch, worker, batch)
                    if ack is None: break
                    await write_packet(writer, {'ack': ack, 'scanned': batch[-1]['seq']})
                elif request.get('type') == 'status' and role == 'control':
                    self.validate_status(worker, request['status'])
                    self.monitor.status(worker, request['status'])
                    await write_packet(writer, {'rotation': {} if self.in_trial() else self.ws_rotation.command(worker)})
                else: raise ValueError('unknown IPC request')
        except (asyncio.IncompleteReadError, ConnectionError, TimeoutError): pass
        except OSError as exc:
            LOG.warning('event=worker_ipc_retry worker=%s reason=%s', worker, exc)
        except (ValueError, KeyError, TypeError, AttributeError, OverflowError, RecursionError) as exc:
            LOG.warning('event=worker_protocol_error worker=%s reason=%s', worker, exc)
            if registered: self.hold_storage(worker, exc)
        except Exception as exc:
            # 解析個別副本時的未預期例外也不能停止其他 IPC；保留原批次供重播。
            LOG.exception('event=worker_ingest_failed worker=%s', worker)
            if registered: self.hold_storage(worker, exc)
        finally:
            if registered: self.connections.discard(connection)
            self.clients.discard(task); writer.close()
            try: await writer.wait_closed()
            except (OSError, ConnectionError): pass

    def commit_batch(self, worker, batch):
        # 目的地暫時無法持久化時保留 worker 的待送資料；恢復未完交易後才回覆 ACK。
        if self.output_fault and time.monotonic() < self.output_retry:
            return None
        try:
            if self.output_fault:
                self.store.recover_output()
                self.store.flush()
            ack = self.store.ingest(worker, batch)
        except (StorageError, sqlite3.Error, OSError) as exc:
            self.output_fault = str(exc)
            self.output_retry = time.monotonic() + 5
            LOG.error('event=supervisor_commit_blocked worker=%s reason=%s action=retain_spool', worker, exc)
            return None
        if self.output_fault:
            LOG.info('event=supervisor_commit_resumed worker=%s ack=%s', worker, ack)
            self.output_fault = None
        return ack

    def validate_status(self, worker, state):
        # 隔離格式損壞的健康封包，避免延後到全組心跳計算時才出錯。
        if not isinstance(state, dict) or state.get('worker') != worker or state.get('identity') != self.expected:
            raise ValueError('invalid worker status identity')
        for field in ('updated', 'started', 'enqueued', 'written', 'ack', 'pending_bytes', 'oldest_pending_s'):
            if type(state.get(field)) not in (int, float) or not math.isfinite(state[field]) or state[field] < 0:
                raise ValueError('invalid worker status: ' + field)
        if not isinstance(state.get('feeds'), dict):
            raise ValueError('invalid worker feeds')
        for values in state['feeds'].values():
            if not isinstance(values, dict) or not isinstance(values.get('session'), str):
                raise ValueError('invalid worker feed session')
            for field in ('received', 'session_started_us', 'last_data_us', 'first_data_us'):
                value = values.get(field)
                if (value is None and field in ('received', 'session_started_us') and field in values
                        or value is not None and (type(value) is not int or value < 0)):
                    raise ValueError('invalid feed status: ' + field)

    async def start_missing(self):
        # 身分不明時重試查詢；確認已退出的副本另建身分，保留原 spool 待補交。
        if self.unsaved_state: self.save()
        while len(self.state['active']) < 2:
            self.state['active'].append(uuid.uuid4().hex[:12]); self.save()
        candidate = self.state['candidate']
        workers = list(self.state['active']) + ([candidate['worker']] if candidate else [])
        deferred = {i.get('replacement') for i in self.state.get('storage_incidents', {}).values()
                    if i.get('start_after', 0) > time.time()}
        for worker in workers:
            if worker in deferred or await self.worker_alive(worker) is not False:
                continue
            if worker in getattr(self.runtime, 'entries', {}):
                self.hold_storage(worker, 'registered worker exited or disappeared')
            else:
                await self.start_worker(worker)
        await self.repair_failed()

    async def rotation(self):
        # 延期不追趕；候選失敗亦等待下一個週期，避免重連耗盡配額。
        if self.state['retiring']:
            await self.finish_retiring(); return
        if self.in_trial():
            reason = 'supervisor_upgrade'
            if time.time()-self.state['last_rotation'] >= self.options['rotation_seconds']:
                self.state['last_rotation'] = time.time()
                self.ws_rotation.log(reason)
            return
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
        # 退役程序尚有待查 spool 時，現役群組仍可處理異常及超齡換線。
        busy = self.monitor.backlogged or bool(self.output_fault) or bool(self.storage_holds())
        await self.locked(self.ws_rotation.tick, now, busy)

    def retire(self, worker, reason):
        # 指令只要求停止收訊，worker 必須先排空交付才自行退出。
        self.state['retiring'] = dict(worker=worker, since=time.time(), reason=reason)
        self.save(); (self.paths['control'] / (worker + '.drain')).touch()
        LOG.info('event=worker_drain_requested worker=%s reason=%s', worker, reason)

    async def finish_retiring(self):
        # 不以期限強殺尚未交付資料；逾時持續告警並暫停下一次換代。
        item = self.state['retiring']; worker = item['worker']
        (self.paths['control'] / (worker + '.drain')).touch()
        alive = await self.worker_alive(worker)
        if alive is None:
            return
        if not alive:
            try:
                if not await self.locked(self.recover_spool_batch, worker):
                    return
                await self.runtime.stop(worker)
            except Exception as exc:
                self.hold_storage(worker, exc)
                return
            await self.locked(self.monitor.forget, worker)
            self.state['retiring'] = None; self.state['last_rotation'] = time.time(); self.save()
            LOG.info('event=worker_retired worker=%s reason=%s', worker, item['reason'])
        elif time.time() - item['since'] > self.options['drain_timeout']:
            LOG.warning('event=worker_drain_pending worker=%s action=retain_spool_and_pause_rotation', worker)

    def recovery_batch(self,db,worker,written,after):
        # 掃描位置可先越過待查事件，清理 spool 仍只使用連續 ACK。
        batch = []; size = 0
        for seq, raw in db.execute('SELECT seq,cast(payload AS BLOB) FROM items WHERE seq>? ORDER BY seq LIMIT 512',(after,)):
            if seq > written:
                raise StorageError(f'retiring spool exceeds durable cursor: worker={worker} seq={seq}')
            if batch and size + len(raw) > self.options['batch_max_bytes']:
                break
            batch.append(dict(seq=seq, record=json.loads(raw))); size += len(raw)
        return batch

    def recover_spool_batch(self, worker):
        # 每輪限一批，讓活躍副本在兩批之間取得提交鎖，且讓主管繼續心跳。
        root = self.paths['spool'] / worker; path = root / 'spool.sqlite'
        receipt = self.paths['control'] / (worker + '.spool-ok.json')
        if not path.exists():
            if receipt.exists():
                if root.exists(): shutil.rmtree(root)
                return True
            if root.exists() or (self.store.cursor(worker) and worker in self.storage_holds()):
                raise StorageError(f'missing worker spool: worker={worker}')
            return True
        if worker not in self.recovery_dbs:
            self.recovery_dbs[worker] = database(path)
        db = self.recovery_dbs[worker]
        written, ack = db.execute('SELECT seq,ack FROM state WHERE id=1').fetchone()
        committed = self.store.cursor(worker)
        if not ack <= committed <= written:
            raise StorageError(f'inconsistent retiring spool: worker={worker} ack={ack} committed={committed} durable={written}')
        after,retry = self.recovery_positions.get(worker,(0,0))
        prefix = time.monotonic()>retry or after>=written
        batch = self.recovery_batch(db,worker,written,committed if prefix else after)
        if batch:
            ack = self.store.ingest(worker, batch)
            self.recovery_positions[worker] = (max(after,batch[-1]['seq']),time.monotonic()+5 if prefix else retry)
            with db:
                db.execute('DELETE FROM items WHERE seq<=?', (ack,))
                db.execute('UPDATE state SET ack=? WHERE id=1', (ack,))
            return False
        if committed != written:
            raise StorageError(f'incomplete retiring spool: worker={worker} committed={committed} durable={written}')
        self.recovery_dbs.pop(worker).close()
        if worker in self.storage_holds():
            # 補交執行緒只發布該副本的收據，避免與主循環同時改寫角色檔。
            publish(receipt, dict(worker=worker, durable=written, committed=committed))
        shutil.rmtree(root)
        return True

    def maintain_files(self):
        # 壓縮與校驗在提交鎖外使用獨立資料庫連線。
        compress_ready(self.root)
        archive_files(self.root,self.store.cold.binding)

    def history_batch(self,limit):
        # 恢復後再重查曾依賴冷盤的缺口，僅讀取本輪要複製的熱資料。
        cold = self.store.cold
        cold.configure()
        if cold.error:
            raise OSError(cold.error)
        db = self.store.db
        if db.execute('SELECT 1 FROM cold_gap_retry LIMIT 1').fetchone():
            cold.connect()
            with db:
                db.execute('INSERT OR IGNORE INTO dirty_gaps SELECT gap FROM cold_gap_retry')
                db.execute('DELETE FROM cold_gap_retry')
        return cold.prepare(limit)

    async def maintain_history(self,limit):
        # 冷盤同步不佔交付鎖，故障時保留中轉副本並於稍後重試。
        try:
            batch = await self.locked(self.history_batch,limit)
            if batch:
                await asyncio.to_thread(self.store.cold.copy_index,batch)
                await self.locked(self.store.cold.finish,batch)
        except (OSError,sqlite3.Error) as exc:
            self.cold_retry = time.monotonic()+30
            LOG.warning('event=cold_index_deferred reason=%s', exc)

    async def maintain_raw(self, now):
        # 壓縮在獨立執行緒讀封段，慢磁碟不會佔住交付鎖；失敗保留原文重試。
        if self.compression and self.compression.done():
            try:
                self.compression.result()
            except Exception:
                LOG.exception('event=raw_compression_failed action=retry')
            self.compression = None
        if self.options['compress_raw'] and not self.compression and now - self.last_compression >= 5:
            self.compression = asyncio.create_task(asyncio.to_thread(self.maintain_files))
            self.last_compression = now
        if now - self.last_archive >= self.options['archive_seconds']:
            started = time.monotonic()
            limit = self.options['archive_batch_size']
            if self.monitor.backlogged:
                limit = min(256,limit)
            moved = await self.locked(self.store.archive.move,self.options['index_hot_seconds'],limit)
            self.last_archive = time.monotonic()
            if self.transfer and self.transfer.done():
                self.transfer.result()
                self.transfer = None
            if not self.transfer and now>=self.cold_retry and (self.store.cold.binding or self.store.cold.error):
                self.transfer = asyncio.create_task(self.maintain_history(limit))
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
            await asyncio.to_thread(self.disks.read,self.store.cold.binding)
            LOG.info('event=storage_health details=%s',json.dumps({k:v for k,v in self.disks.latest.items() if k!='recent_connections'}))
            self.repair, gaps = await self.locked(self.monitor.health, self.workers(), self.state['active']); self.last_health = now
            for gap, feed in await self.locked(self.monitor.snapshot_requests):
                market, symbol, _ = feed.split('/'); await self.schedule.call('request_snapshot', market, symbol)
                await self.locked(self.monitor.snapshot_requested, gap)
            report = dict(self.monitor.latest, supervisor_id=self.supervisor_id, commit_blocked=self.output_fault, active=list(self.state['active']), containers={w: bool(await self.worker_alive(w)) for w in self.workers()})
            if self.storage_holds():
                LOG.warning('event=worker_storage_hold workers=%s action=inspect_worker_logs', sorted(self.storage_holds()))
            atomic_json(self.root / 'status.json', self.storage_report(report))
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

    async def publish_notices(self, stopped):
        # 通知已隨行情交易落盤；慢速輸出在提交鎖外執行，讓其他批次先取得 ACK。
        while not stopped.is_set():
            rows = await self.locked(self.store.pending_notices)
            if rows:
                await asyncio.to_thread(self.store.print_notices, rows)
                await self.locked(self.store.confirm_notices, rows[-1][0])
            else:
                try: await asyncio.wait_for(stopped.wait(), .5)
                except asyncio.TimeoutError: pass

    async def serve(self, duration=None):
        # 重啟 supervisor 只重建 socket；既有 worker 自動重送與追趕。
        self.paths['socket'].unlink(missing_ok=True)
        server = await asyncio.start_unix_server(self.accept, path=str(self.paths['socket']))
        started = time.monotonic()
        if self.monitor.blockers:
            LOG.warning('event=rotation_preflight scope=group policy=strict blocked=%s action=retain_unverified_sessions', json.dumps(self.monitor.blockers))
        notice_stop = asyncio.Event()
        publisher = asyncio.create_task(self.publish_notices(notice_stop))
        try:
            while not self.stop.is_set():
                if publisher.done(): publisher.result()
                try:
                    if not self.in_trial():
                        await self.start_missing()
                    await self.background()
                    await self.rotation()
                except (sqlite3.OperationalError, OSError, RuntimeUnavailable) as exc:
                    LOG.error('event=supervisor_maintenance_retry reason=%s', exc)
                    await asyncio.sleep(1)
                if self.on_tick and self.on_tick(self):
                    self.detach = True; break
                if duration and time.monotonic() - started >= duration: break
                try: await asyncio.wait_for(self.stop.wait(), .5)
                except TimeoutError: pass
            if not self.detach: await self.shutdown()
        finally:
            # Python 3.12 會等現有 socket 關閉；先取消處理器，避免 wait_closed 互等。
            server.close(); clients = list(self.clients)
            for task in clients: task.cancel()
            await asyncio.gather(*clients, return_exceptions=True)
            await server.wait_closed()
            if self.compression: await asyncio.gather(self.compression, return_exceptions=True)
            if self.transfer: await self.transfer
            for db in self.recovery_dbs.values():
                await asyncio.to_thread(db.close)
            self.recovery_dbs.clear()
            notice_stop.set()
            await publisher

    async def sync_cold_manifest(self):
        # 停止報告亦複製到冷盤，離線時保留熱盤權威版本。
        if self.store.cold.binding:
            try:
                await asyncio.to_thread(archive_files,self.root,self.store.cold.binding)
            except (OSError,sqlite3.Error) as exc:
                LOG.warning('event=cold_manifest_deferred reason=%s',exc)

    async def shutdown(self):
        # 使用者正常停止時排空所有副本；程序崩潰不會走此路徑。
        workers = self.workers()
        for worker in workers: (self.paths['control'] / (worker + '.drain')).touch()
        deadline = time.monotonic() + self.options['drain_timeout'] + 5
        last_report = 0
        while any([await self.worker_alive(w) for w in workers]) and time.monotonic() < deadline:
            if time.monotonic() - last_report >= self.options['heartbeat_seconds']:
                await self.locked(self.monitor.health, workers, self.state['active']); last_report = time.monotonic()
                LOG.info('event=shutdown_progress phase=draining_workers')
            await asyncio.sleep(.2)
        for worker in workers:
            if await self.worker_alive(worker) is None:
                self.hold_storage(worker, 'cannot verify worker shutdown; retain role and spool')
                continue
            await self.runtime.stop(worker)
            while not await self.locked(self.recover_spool_batch, worker):
                if (self.store.db.execute('SELECT 1 FROM waiting WHERE worker=? LIMIT 1',(worker,)).fetchone()
                        and self.recovery_positions[worker][0] >= self.recovery_dbs[worker].execute('SELECT seq FROM state').fetchone()[0]):
                    self.hold_storage(worker,'cold history pending; retain durable spool')
                    break
                await asyncio.sleep(0)
            await self.locked(self.monitor.forget, worker)
        held = self.storage_holds()
        self.state['active'] = [w for w in self.state['active'] if w in held]
        for role in ('candidate', 'retiring'):
            if self.state[role] and self.state[role]['worker'] not in held:
                self.state[role] = None
        self.state['last_rotation'] = time.time(); self.save()
        await self.locked(self.store.seal)
        if self.compression:
            await asyncio.gather(self.compression, return_exceptions=True)
            self.compression = None
        if self.options['compress_raw']:
            while await asyncio.to_thread(compress_ready, self.root):
                pass
        await self.locked(self.monitor.health, [])
        await self.locked(self.monitor.stopped, sorted(held))
        self.storage_report(self.monitor.latest)
        atomic_json(self.root / 'status.json', self.monitor.latest)
        self.manifest.update(status=self.monitor.latest['status'], ended_us=time.time_ns() // 1000,
                             raw_records=self.monitor.latest['raw_records'], health_at_stop=self.monitor.latest)
        atomic_json(self.root / 'manifest.json', self.manifest)
        await self.sync_cold_manifest()


async def supervise_generation(leadership, args, owner):
    # 僅持有正式檔鎖者可開啟資料庫；所有替補仍由該批次固定的 worker 映像啟動。
    root, config = leadership.root, leadership.config
    expected = owner.get('worker_identity')
    if owner:
        leadership.check(owner)
        if not leadership.trial and owner['code'] != leadership.code:
            raise ValueError('use a new supervisor ID and --takeover for a different version')
    image = expected['image'] if expected else args.image
    runtime = Runtime(root, config, args.backend, image, leadership.mount)
    supervisor = file_log = reader_lock = None
    try:
        if leadership.data_lock != leadership.writer:
            reader_lock = acquire(leadership.data_lock)
            if not reader_lock: raise RuntimeError('run already locked by collector or replay')
        await runtime.open()
        if expected and runtime.image != expected['image']:
            raise ValueError('pinned worker image unavailable')
        path = leadership.control / 'config.json'
        if path.exists() and read(path) != config:
            raise ValueError('existing run configuration differs')
        if not path.exists(): publish(path, config)
        supervisor = Supervisor(root, config, runtime, expected)
        supervisor.supervisor_id = leadership.name
        supervisor.on_tick = leadership.tick
        supervisor.in_trial = lambda: leadership.trial is not None
        file_log = configure_logging(root / 'supervisor.log', config['log_max_bytes'], config['log_backup_count'])
        if not owner:
            publish(leadership.owner, leadership.record(supervisor.expected))
        if leadership.trial:
            proof = leadership.trial['proof']
            proof['seq'] = supervisor.store.db.execute('SELECT written FROM output').fetchone()[0]
            for worker in proof['paths']:
                proof['paths'][worker]['ack'] = max(proof['paths'][worker]['ack'], supervisor.store.cursor(worker))
        await supervisor.serve(args.duration)
        if leadership.trial: return 75
        return 74 if supervisor.storage_holds() and not supervisor.detach else 0
    finally:
        try:
            if supervisor:
                supervisor.schedule.close()
                supervisor.store.close()
        finally:
            await runtime.close()
            close_log(file_log)
            if reader_lock: reader_lock.close()


async def run_supervisor(args):
    # 主管正常停機會排空；試接與待命中的程序停止時不會向收訊副本送出停止指令。
    root = Path(args.output).resolve()
    control = root / '_state'
    control.mkdir(parents=True, exist_ok=True)
    config = group_config(args.exchange, args.config)
    leadership = Leadership(root, args, config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, leadership.cancel.set)
    try:
        return await leadership.manage(args.takeover, lambda owner: supervise_generation(leadership, args, owner))
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
