"""所有交易所與 A/B/C 共用的 worker：原生收訊、暫存、ACK 交付。"""
import asyncio
import base64
import json
import os
from pathlib import Path
import random
import signal
import time
import uuid
import aiohttp
from common import LOG
from .transport import adapter, key, proxy_url, reason, retry_after
from .delivery import Spool, read_packet, write_packet
from .redundancy import feed_groups, identity, local_paths, source_inventory
from .replica_protocol import subscription, classify_group, atoms
from .rest_schedule import Schedule


class Worker:
    """角色差異只在執行個體識別，沒有 A 或 B 專屬分支。"""
    def __init__(self, root, config, worker_id, image):
        # 讀取同一封存配置，恢復待交付資料與共用排程。
        self.root = Path(root); self.config = config; self.worker_id = worker_id
        self.options = config['redundancy']; self.paths = local_paths(root)
        self.identity = identity(config, image); self.stop = asyncio.Event()
        self.spool = Spool(self.paths['spool'] / worker_id, self.options)
        self.schedule = Schedule(self.paths['control'] / 'schedule.sqlite', config)
        self.groups = feed_groups(config); self.repair = {}; self.draining = False; self.ack_ms = 0
        self.route_cooldowns = {}
        self.stats = {name: dict(connected=False, subscribed=False, received=0, session='', last_data_us=None)
                      for name in source_inventory(config)}
        self.proxy = proxy_url(config.get('proxy')); self.started = time.time()
        self.generation = uuid.uuid4().hex

    def event(self, feed, name, **details):
        # 狀態只交給 supervisor 記錄，正式資料檔不包含 worker 控制訊息。
        self.spool.emit(dict(kind='event', event=name, details=details, **feed,
                             local_timestamp=time.time_ns() // 1000))

    async def stream(self, group):
        # 每組連線獨立重試；來源靜默不直接引發整個 worker 重啟。
        failures = 0
        while not self.stop.is_set():
            excluded = [url for (name,url),until in self.route_cooldowns.items() if name == group['id'] and until > time.monotonic()]
            endpoint = await self.schedule.call('endpoint', group, excluded)
            if not endpoint or not await self.schedule.call('connection', group['feeds'][0]['exchange']):
                await asyncio.sleep(1); continue
            session = uuid.uuid4().hex; received = 0
            self.event(group['feeds'][0], 'connect_attempt', endpoint=endpoint, group=group['id'], session=session)
            for feed in group['feeds']:
                self.stats[key(feed)].update(connected=False, subscribed=False, session=session, last_data_us=None, first_data_us=None,
                                             session_started_us=time.time_ns() // 1000, received=0)
            repaired = False; error = None
            try:
                async with self.ws_http.ws_connect(endpoint, params=adapter(group['feeds'][0]['exchange']).query(None, group['feeds'][0]),
                        autoping=False, heartbeat=None, proxy=self.proxy, max_msg_size=8 * 1024**2) as ws:
                    received = await self.receive(ws, group, endpoint, session)
            except (aiohttp.ClientError, OSError, TimeoutError, ValueError) as exc:
                repaired = str(exc) == 'peer_ahead_repair'
                error = exc
                self.event(group['feeds'][0], 'disconnected', group=group['id'], reason=reason(exc))
            finally:
                for feed in group['feeds']: self.stats[key(feed)]['connected'] = False
            valid = any(self.stats[key(f)]['last_data_us'] for f in group['feeds'])
            if not valid and not repaired: await self.route_failed(group, endpoint, error, failures)
            failures = 0 if valid else failures + 1
            await asyncio.sleep(random.uniform(.5, min(30, 2 ** min(failures + 1, 5))))

    async def route_failed(self, group, endpoint, error, failures):
        # 網路暫時失敗只冷卻本 worker，不能讓另一條健康連線的路由被全域封鎖。
        protocol = isinstance(error, aiohttp.WSServerHandshakeError) and error.status in (400,404,410,426)
        if str(error) == 'route_probe_no_matching_data' or protocol:
            await self.schedule.call('route_result', group, endpoint, False)
        self.route_cooldowns[(group['id'],endpoint)] = time.monotonic() + min(30,2 ** min(failures+1,5))

    async def receive(self, ws, group, endpoint, session):
        # 主動 ping 不受行情流量影響；資料與雙向連線健康分開判斷。
        for feed in group['feeds']: self.stats[key(feed)]['connected'] = True
        await ws.send_json(subscription(group))
        started = last_ping = last_app = time.monotonic(); pong_deadline = None
        receive = asyncio.create_task(ws.receive()); count = 0
        try:
            while not self.stop.is_set():
                now = time.monotonic()
                if not count and now - started > 20 and group['feeds'][0]['channel'] in ('trade', 'bookTicker', 'depth@0ms', 'depth@100ms', 'ticker', 'markPrice@1s'):
                    raise TimeoutError('route_probe_no_matching_data')
                if self.repair_due(group, session): raise ValueError('peer_ahead_repair')
                if pong_deadline and now > pong_deadline: raise TimeoutError('ping_timeout')
                if now - last_ping >= 20:
                    await ws.ping(); last_ping = now; pong_deadline = now + 10
                heartbeat = adapter(group['feeds'][0]['exchange']).application_heartbeat
                if heartbeat and now - last_app >= 20:
                    await ws.send_str(heartbeat); last_app = now
                done, _ = await asyncio.wait([receive], timeout=.5)
                if not done:
                    self.check_subscription(group, started); continue
                msg = receive.result(); receive = asyncio.create_task(ws.receive())
                if msg.type == aiohttp.WSMsgType.PING: await ws.pong(msg.data); self.control(group); continue
                if msg.type == aiohttp.WSMsgType.PONG: pong_deadline = None; self.control(group); continue
                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    raise ConnectionError(f'websocket_closed code={ws.close_code} error={ws.exception()}')
                previous = count; count += self.message(msg, group, endpoint, session)
                if not previous and count: await self.schedule.call('route_result', group, endpoint, True)
                self.check_subscription(group, started)
            return count
        finally:
            receive.cancel(); await asyncio.gather(receive, return_exceptions=True)

    def repair_due(self, group, session):
        # 指令只適用於觀察時的連線；恢復收訊或過期後不得再中斷新連線。
        command = self.repair.pop(group['id'], None)
        if not command or command['session'] != session or time.time() > command['expires']: return False
        newest = max(self.stats[key(f)].get('last_data_us') or 0 for f in group['feeds'])
        return newest <= command['last_data_us']

    def control(self, group):
        # 控制封包只更新傳輸健康，不增加行情筆數或交接共同事件數。
        for feed in group['feeds']: self.stats[key(feed)]['last_control_us'] = time.time_ns() // 1000

    def check_subscription(self, group, started):
        # 訂閱逾時只處理有 ACK 的交易所；不能因冷門商品沒成交就斷線。
        cls = adapter(group['feeds'][0]['exchange'])
        if cls.requires_ack and time.monotonic() - started > 15:
            if any(not self.stats[key(f)]['subscribed'] for f in group['feeds']):
                raise TimeoutError('subscription_ack_timeout')

    def message(self, msg, group, endpoint, session):
        # 原文只存一次；解析失敗及非行情訊息交給狀態日誌。
        if msg.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY): return 0
        received = time.time_ns() // 1000; binary = isinstance(msg.data, bytes)
        if msg.data in ('pong', b'pong'): self.control(group); return 0
        obj = json.loads(msg.data)
        kind, matching = classify_group(group, obj)
        if kind == 'error': raise ValueError(str(obj))
        if kind == 'ack':
            self.control(group)
            for feed in matching: self.stats[key(feed)]['subscribed'] = True
            return 0
        if kind != 'data':
            self.application_control(group, obj)
            return 0
        feed = matching[0]
        record = dict(feed, kind='ws', session=session, local_timestamp=received, endpoint=endpoint,
                      worker_generation=self.generation, encoding='base64' if binary else 'utf8',
                      raw=base64.b64encode(msg.data).decode() if binary else msg.data)
        record.pop('urls', None)
        # 完整驗證與事件指紋交由 supervisor 一次執行，收訊端完整保留原文。
        self.spool.emit(record)
        if self.stats[key(feed)]['last_data_us'] is None:
            self.stats[key(feed)]['first_data_us'] = received
            self.event(feed, 'first_message', session=session, endpoint=endpoint)
        self.stats[key(feed)].update(subscribed=True, last_data_us=received,
                                     received=self.stats[key(feed)]['received'] + 1)
        return 1

    def application_control(self, group, obj):
        # Coinbase 心跳保留原生交易進度作觀察，不能把全市場序號當作連續成交序號。
        if not isinstance(obj, dict): return
        if obj.get('type') == 'heartbeat':
            for feed in group['feeds']:
                if feed['symbol'] == obj.get('product_id'):
                    self.stats[key(feed)].update(last_control_us=time.time_ns() // 1000,
                                                 heartbeat_trade_id=obj.get('last_trade_id'), heartbeat_sequence=obj.get('sequence'))
        elif obj.get('op') == 'pong' or obj.get('ret_msg') == 'pong': self.control(group)

    async def rest_loop(self):
        # 租約與預算跨副本共用；只有本次 claim 的 worker 發送請求。
        while not self.stop.is_set():
            job = await self.schedule.call('claim', self.worker_id, self.rest_ready)
            if job is None:
                await asyncio.sleep(.2); continue
            try: await self.request(job)
            except (aiohttp.ClientError, TimeoutError, ValueError, OSError) as exc:
                await self.schedule.call('finish', self.worker_id, job, False)
                self.event(dict(exchange=job['market'], symbol=job['symbol'], channel=job['channel']),
                           'rest_error', reason=reason(exc))

    def rest_ready(self, market, symbol, channel):
        # 初始及定期深度快照都必須晚於本路訂閱收訊，否則快照無法銜接。
        if channel != 'depthSnapshot': return True
        depth = 'depth@100ms' if market == 'binance' else 'depth@0ms'
        if depth not in self.config['market_settings'][market]['channels']: return True
        state = self.stats.get('/'.join((market, symbol, depth)), {})
        return bool(state.get('connected') and state.get('subscribed') and state.get('last_data_us'))

    async def request(self, job):
        # REST 原文與請求邊界一起保存，供離線 mapper 重建快照前緩存。
        channel, market = job['channel'], job['market']
        suffix = '/depth' if channel == 'depthSnapshot' else '/' + channel
        endpoint = self.config['rest'][market] + suffix
        params = {'symbol': job['symbol']} if job['symbol'] else {}
        if channel == 'depthSnapshot': params['limit'] = 1000
        started = time.time_ns() // 1000
        async with self.rest_http.get(endpoint, params=params, proxy=self.proxy,
                timeout=aiohttp.ClientTimeout(total=self.config['rest_timeout'])) as response:
            raw = await response.text(); received = time.time_ns() // 1000
            if response.status != 200:
                pause = retry_after(response.headers.get('Retry-After')) if response.status in (429, 418) else 5
                await self.schedule.call('finish', self.worker_id, job, False, pause)
                self.event(dict(exchange=market, symbol=job['symbol'], channel=channel),
                           'rest_rate_limited' if response.status in (429, 418) else 'rest_error',
                           status=response.status, cooldown=pause)
                return
            data = json.loads(raw)
            if channel == 'exchangeInfo': await self.schedule.call('metadata', market, data)
            else:
                record = dict(kind='rest', exchange=market, symbol=job['symbol'], channel=channel, raw=raw,
                              time_unit='ms', generated=True, session='rest', request_id=job['request_id'],
                              generation=started, worker_generation=self.generation, endpoint=endpoint,
                              request_started_us=started, local_timestamp=received)
                atoms(record); sequence = self.spool.emit(record)
                # 租約完成前確保原文已進持久緩衝，避免下輪排程掩蓋未落盤回應。
                while self.spool.written < sequence:
                    if self.spool.error: raise self.spool.error
                    await asyncio.sleep(.01)
                self.stats[key(record)].update(connected=True, subscribed=True, last_data_us=received,
                                               received=self.stats[key(record)]['received'] + 1)
            await self.schedule.call('finish', self.worker_id, job, True, headers=dict(response.headers))

    def status(self):
        # 每個來源保留自己的收訊時間；REST 的健康由共用排程與 supervisor 彙總。
        if self.spool.error: raise self.spool.error
        command = self.paths['control'] / (self.worker_id + '.drain')
        if command.exists(): self.stop.set()
        return dict(worker=self.worker_id, pid=os.getpid(), started=self.started, updated=time.time(), identity=self.identity,
                    draining=self.draining, feeds=self.stats, enqueued=self.spool.seq, written=self.spool.written,
                    ack=self.spool.ack, pending_bytes=self.spool.bytes, memory_bytes=self.spool.queued_bytes,
                    oldest_pending_s=max(0, time.time() - self.spool.oldest_us / 1e6) if self.spool.oldest_us else 0,
                    ack_ms=self.ack_ms)

    @staticmethod
    def check_welcome(answer):
        # 舊 IPC 尚未關閉只需重試；版本或設定拒絕才屬於不可接管錯誤。
        if answer.get('retry'): raise ConnectionError(answer['error'])
        if answer.get('error'): raise ValueError(answer['error'])

    async def control_sender(self):
        # 獨立 IPC 傳遞健康，資料批次等待 ACK 不會拖延狀態或修復指令。
        while True:
            try:
                reader, writer = await asyncio.open_unix_connection(str(self.paths['socket']))
                try:
                    await write_packet(writer, dict(type='hello', role='control', worker=self.worker_id, identity=self.identity))
                    answer = await read_packet(reader)
                    self.check_welcome(answer)
                    while True:
                        await write_packet(writer, dict(type='status', status=self.status()))
                        response = await asyncio.wait_for(read_packet(reader), 10)
                        self.repair.update({r['group']: r for r in response.get('repair', [])})
                        await asyncio.sleep(1)
                finally:
                    writer.close(); await writer.wait_closed()
            except (OSError, asyncio.IncompleteReadError, ConnectionError, TimeoutError): await asyncio.sleep(1)

    async def sender(self):
        # supervisor 斷線只影響交付；所有未確認資料持續留在 spool。
        while True:
            try:
                reader, writer = await asyncio.open_unix_connection(str(self.paths['socket']))
                try:
                    await write_packet(writer, dict(type='hello', worker=self.worker_id, identity=self.identity))
                    answer = await read_packet(reader)
                    self.check_welcome(answer)
                    await self.send_connected(reader, writer)
                finally:
                    writer.close(); await writer.wait_closed()
            except (OSError, asyncio.IncompleteReadError, ConnectionError, TimeoutError) as exc:
                LOG.warning('event=supervisor_unavailable worker=%s reason=%s', self.worker_id, str(exc))
                await asyncio.sleep(1)

    async def send_connected(self, reader, writer):
        # 先送狀態再送資料；ACK 只能由正式持久化進度推進。
        while True:
            batch = await asyncio.to_thread(self.spool.pending)
            if not batch:
                await asyncio.sleep(.1); continue
            await write_packet(writer, dict(type='batch', batch=batch))
            started = time.monotonic()
            response = await asyncio.wait_for(read_packet(reader), 60)
            if response.get('error'): raise ValueError(response['error'])
            await asyncio.to_thread(self.spool.acknowledge, response['ack'])
            self.ack_ms = round((time.monotonic() - started) * 1000, 3)

    async def watchdog(self):
        # sender 失聯時仍檢查磁碟錯誤及 drain 指令，不依賴 supervisor 回應。
        while not self.stop.is_set():
            self.status()
            await asyncio.sleep(1)

    async def execute(self):
        # WS 與 REST 分開連線池，避免長連線佔滿 REST 的連線名額。
        timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10)
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0), timeout=timeout) as ws_http:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=10), timeout=timeout) as rest_http:
                self.ws_http = ws_http; self.rest_http = rest_http
                await self.lifecycle()

    async def lifecycle(self):
        # 正常 drain 先停止收訊，再排空 spool；逾時仍保留磁碟待交付資料。
        sender = asyncio.create_task(self.sender()); watcher = asyncio.create_task(self.watchdog())
        control = asyncio.create_task(self.control_sender())
        captures = [asyncio.create_task(self.stream(group)) for group in self.groups]
        captures += [asyncio.create_task(self.rest_loop()) for _ in range(2)]
        stop = asyncio.create_task(self.stop.wait())
        try:
            done, _ = await asyncio.wait([sender, watcher, control, stop, *captures], return_when=asyncio.FIRST_COMPLETED)
            for task in done: task.result()
        finally:
            self.draining = True; self.stop.set()
            for task in [watcher, stop, *captures]: task.cancel()
            await asyncio.gather(watcher, stop, *captures, return_exceptions=True)
            try: await self.finish(sender)
            finally:
                control.cancel(); await asyncio.gather(control, return_exceptions=True)

    async def finish(self, sender):
        # 儲存失敗也必須回收傳輸任務，不能讓清理例外留下背景連線。
        try:
            await asyncio.to_thread(self.spool.close)
            deadline = time.monotonic() + self.options['drain_timeout']
            while self.spool.ack < self.spool.written and time.monotonic() < deadline and not sender.done():
                await asyncio.sleep(.1)
        finally:
            sender.cancel(); await asyncio.gather(sender, return_exceptions=True)
            self.schedule.close(); self.spool.db.close()


async def run_worker(root, worker_id, image):
    # 所有 worker 使用同一入口；身分及路徑由 supervisor 指派。
    config = json.loads((Path(root) / '_state' / 'config.json').read_text())
    worker = Worker(root, config, worker_id, image)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT): loop.add_signal_handler(sig, worker.stop.set)
    await worker.execute()
