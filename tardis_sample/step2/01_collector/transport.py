"""採集共用設定、網路傳輸、限流與健康監控。"""
import asyncio
import base64
from collections import Counter, deque
from email.utils import parsedate_to_datetime
from functools import lru_cache
from importlib import import_module
import json
import logging
import os
from pathlib import Path
import random
import time
from urllib.parse import urlsplit
import uuid

import aiohttp

from common import LOG, ProtocolError, STEP2_ROOT, atomic_json
from common.protocol import ADAPTERS

COLLECTOR_ROOT = Path(__file__).resolve().parent
METADATA = STEP2_ROOT.parent / 'exchange_info'


def proxy_url(setting):
    # 統一解析 WS／REST 代理設定；只接受支援的 HTTP／HTTPS 代理。
    if setting == 'environment':
        setting=next((os.environ[k] for k in ('https_proxy','HTTPS_PROXY','http_proxy','HTTP_PROXY','all_proxy','ALL_PROXY') if os.environ.get(k)),None)
    if setting and urlsplit(setting).scheme not in ('http','https'):
        raise ValueError('proxy must use http/https; SOCKS requires a separate connector')
    return setting


@lru_cache(maxsize=None)
def adapter(market):
    # 依市場 ID 載入專屬 Collector，避免將不相容協議混入共用傳輸層。
    return import_module(f'.collectors.{ADAPTERS[market]}', __package__).Collector


def configuration(path=None, markets=None, symbols=None):
    # 載入單一設定檔、檢查參數與商品代碼，再讓各交易所驗證自己的來源 metadata。
    settings = json.loads((Path(path) if path else COLLECTOR_ROOT / 'settings.json').read_text())
    settings.setdefault('log_max_bytes', 10 * 1024 * 1024)
    settings.setdefault('log_backup_count', 5)
    settings['markets'] = markets or settings['markets']
    selected = settings['markets']
    if not selected or not set(selected) <= ADAPTERS.keys() or len(selected) != len(set(selected)):
        raise ValueError('unknown or duplicate markets')
    cls = adapter(selected[0])
    if len(selected) > 1 and (not cls.legacy_multi_market or any(adapter(m) is not cls for m in selected)):
        raise ValueError('run these markets separately; symbol/protocol conventions differ')
    settings['symbols'] = symbols or cls.legacy_symbols(settings, selected[0])
    if not settings['symbols'] or len(settings['symbols']) != len(set(settings['symbols'])):
        raise ValueError('empty or duplicate symbols')
    if any(not s.isascii() or not s.replace('-', '').isalnum() for s in settings['symbols']):
        raise ValueError('invalid symbol')
    for key in ('queue_capacity','min_free_gib','segment_seconds','snapshot_interval','oi_interval','heartbeat_seconds','ws_max_age','rest_timeout','ws_attempts_per_5min'):
        if settings[key] <= 0: raise ValueError(f'{key} must be positive')
    for key in ('log_max_bytes', 'log_backup_count'):
        if not isinstance(settings[key], int) or settings[key] < 1:
            raise ValueError(f'{key} must be a positive integer')
    settings['metadata'] = {}
    for market in selected:
        adapter(market).configure(settings, market)
    return settings


def feeds(config):
    # 把市場／商品設定展開成原生 WS feed，每個 feed 保持獨立接收順序與 session。
    return [f for m in config['markets'] for f in adapter(m).feeds(config, m)]


class RetryFeed(RuntimeError): 
    """可由單一 feed 退避重連處理的狀態，例如靜默、斷線或定期輪換。"""
    pass
class HTTPFailure(RuntimeError): 
    """REST 回傳非成功 HTTP 狀態；與資料格式錯誤分開記錄。"""
    pass

def key(feed): 
    # 產生 market/symbol/channel 的穩定識別字，供狀態與日誌共同使用。
    return '/'.join(feed[k] for k in ('exchange', 'symbol', 'channel'))
def reason(exc):
    # 保留例外類型、訊息、errno 與原因鏈，方便區分網路與協議故障。
    return {'type': type(exc).__name__, 'message': str(exc), 'errno': getattr(exc, 'errno', None),
            'cause': repr(exc.__cause__) if exc.__cause__ else None}
def retry_after(value, default=30):
    # 解析 Retry-After 秒數或 HTTP 日期；無法解析時使用保守預設值。
    try: return max(0, float(value))
    except (ValueError, TypeError):
        try: return max(0, parsedate_to_datetime(value).timestamp() - time.time())
        except (ValueError, TypeError, OverflowError): return default


def websocket_timeout(close_seconds=2):
    # 新版使用專用設定；舊版以秒數指定關閉逾時，保持相同語義。
    timeout_type = getattr(aiohttp, 'ClientWSTimeout', None)
    if timeout_type is not None:
        return timeout_type(ws_close=close_seconds)
    return float(close_seconds)

class Budget:
    """以單調時鐘維護滑動視窗配額，並共用交易所的限流冷卻期限。"""
    def __init__(self, capacity, period=60):
        # 設定視窗容量與時間，建立只保護配額計算的短生命週期鎖。
        self.capacity=capacity; self.period=period; self.used=deque(); self.lock=asyncio.Lock(); self.cooldown=0
    async def acquire(self, weight):
        # 等待配額與冷卻結束；等待期間釋放鎖，不把網路請求包在全域鎖內。
        while True:
            async with self.lock:
                now=time.monotonic()
                while self.used and self.used[0][0]<=now-self.period: self.used.popleft()
                wait=max(0,self.cooldown-now)
                if sum(n for _,n in self.used)+weight>self.capacity: wait=max(wait,self.used[0][0]+self.period-now)
                if wait<=0: self.used.append((now,weight)); return
            await asyncio.sleep(min(wait,5))

class BaseCollector:
    """共用 WS 生命週期與健康監控；訂閱、ACK 與 payload 語義由子類別處理。"""
    capability = 'sample_raw_only'
    connection_spacing = .25
    requires_ack = True
    application_heartbeat = None
    replica_group_limit = 100
    continuity_channels = {}
    replica_silence_seconds = {}
    connection_limits = ((300, 150),)
    legacy_multi_market = False
    optional_channels = ()
    probe_channels = ()
    native_fields = {}
    variant_fields = {}
    rest_policy = None
    time_unit = 'ms'
    batch_field = None
    unwrap_single = False

    @classmethod
    def feeds(cls, config, market):
        # 同一路由的商品和頻道共用展開方式，端點及事件時間單位由交易所提供。
        return [dict(exchange=market, symbol=symbol, channel=channel, time_unit=cls.time_unit,
                     urls=cls.endpoints(market))
                for symbol in config['symbols'] for channel in cls.channels(market)]

    @classmethod
    def subscription(cls, feed):
        # 單路與分組連線共用同一份原生訂閱格式。
        return cls.replica_subscription([feed])

    @classmethod
    def legacy_symbols(cls, config, market):
        # 舊設定未指定交易所商品時，由該市場提供預設值。
        return cls.default_symbols(market)

    def polling_tasks(self, config):
        # 只有宣告輪詢的交易所需要建立額外任務。
        return []

    @staticmethod
    def control_fields(feed, obj):
        # 有應用層心跳的交易所可回傳要更新的觀察欄位。
        return None

    @staticmethod
    def validate_event(data, channel, symbol):
        # 有欄位驗證規則的市場可在建立事件身分前檢查原文。
        pass

    @staticmethod
    def ws_asset(symbol):
        # 有分隔符的市場以第一段為基礎資產，例如 BTC-USDT-SWAP。
        if '-' in symbol:
            return symbol.partition('-')[0]
        # 連寫商品須完整匹配報價資產，避免將 ETHFIUSDT 或 BTCDOMUSDT 認成 ETH 或 BTC。
        quotes = ('USDT', 'USDC', 'FDUSD', 'USD', 'BUSD', 'TUSD', 'USDP', 'DAI',
                  'BTC', 'ETH', 'BNB', 'EUR', 'TRY', 'BRL', 'AUD', 'GBP', 'JPY', 'PLN', 'ZAR')
        if symbol[:3] in ('BTC', 'ETH') and symbol[3:] in quotes:
            return symbol[:3]
        return None

    @classmethod
    def replica_limit(cls, market):
        # 分組上限由各 adapter 宣告，跨市場差異可覆寫此方法。
        return cls.replica_group_limit
    @staticmethod
    def default_symbols(market): 
        # 提供其他市場示範商品；特殊商品代碼由交易所子類別覆寫。
        return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']
    @classmethod
    def configure(cls, config, market):
        # 標示尚屬原始採集示範的能力邊界，不假裝已完成正規化與 book recovery。
        config['metadata'][market] = {'capability': cls.capability, 'channels': cls.channels(market),
                                      'note': 'Migrated public WS sample; normalization and book recovery not implemented.'}
    def __init__(self,config,root,writer,http):
        # 初始化共用代理、狀態、配額與 REST 併發上限。
        self.config=config; self.root=root; self.writer=writer; self.http=http; self.stats={}; self.events=Counter()
        self.proxy=proxy_url(config['proxy']); self.start=time.monotonic()
        self.budgets={m:Budget(config['rest_weight_per_minute'].get(m,1200)) for m in config['markets']}
        self.ws_budgets={m:Budget(config['ws_attempts_per_5min'],300) for m in config['markets']}
        self.rest_slots=asyncio.Semaphore(6)
    def stat(self,feed):
        # 取得或建立每條 feed 的計數、連線、訂閱與最近有效資料狀態。
        return self.stats.setdefault(key(feed),{'received':0,'connected':False,'subscribed':False,'last_valid_us':None,'last_valid_mono':None,
            'attempts':0,'last_error':None,'endpoint':None,'unavailable_since_us':time.time_ns()//1000})
    def emit(self,feed,kind,session='',**kwargs):
        # 將原始訊息或控制事件交給同一個有序 writer，附上 feed 識別與時間單位。
        return self.writer.emit(kind,feed['exchange'],feed['symbol'],feed['channel'],session,time_unit=feed['time_unit'],**kwargs)
    def event(self,feed,event,session='',**kwargs):
        # 把生命週期事件同時寫入 raw 與日誌；故障用 WARNING，正常轉換用 INFO。
        self.events[event]+=1; self.emit(feed,'event',session,event=event,**kwargs)
        level = logging.WARNING if event.endswith('error') or event in ('disconnected','depth_sequence_gap') else logging.INFO
        LOG.log(level, 'event=%s feed=%s session=%s details=%s', event, key(feed), session, kwargs)
    def valid(self,feed,received):
        # 更新有效資料統計，只在首筆或故障恢復時加日誌，避免每筆行情洗版。
        s=self.stat(feed)
        if not s['received']:
            LOG.info('event=first_message feed=%s received_us=%s', key(feed), received)
        elif s['unavailable_since_us'] is not None:
            LOG.info('event=feed_recovered feed=%s interruption_s=%.3f', key(feed), max(0, received-s['unavailable_since_us'])/1e6)
        s['received']+=1; s['last_valid_us']=received; s['last_valid_mono']=time.monotonic(); s['unavailable_since_us']=None
    def query(self,feed): 
        # 提供 WS URL 查詢參數擴充點；預設不加參數。
        return {}
    def context(self,feed): 
        # 建立每個連線 session 的專屬採集狀態，斷線後不沿用舊序號。
        return {}
    def tasks(self,ctx): 
        # 列出交易所附帶的非同步任務，例如 REST snapshot；預設沒有。
        return []
    async def maintenance(self,feed,session,ctx,now): 
        # 訂閱確認後的維護擴充點，例如發起 REST 快照請求。
        pass
    def completed(self,feed,session,ctx,done): 
        # 處理已完成的交易所附帶任務；預設沒有附加工作。
        pass
    def on_data(self,feed,session,ctx,data,received): 
        # 交易所資料驗證與狀態更新擴充點；原始保存已在此前完成。
        pass
    def stale_limit(self,feed):
        # 依來源設定資料新鮮度門檻；清算沒有事件時不套用行情靜默限制。
        ch=feed['channel']
        return None if ch in self.optional_channels else 120

    def source_healthy(self, market, channel, state, age):
        # 一般來源必須已訂閱並收到行情，稀少事件則允許靜默。
        limit = self.stale_limit(dict(exchange=market, channel=channel))
        fresh = age is not None and (limit is None or age <= limit)
        return state['connected'] and state['subscribed'] and (channel in self.optional_channels or fresh)

    async def stream(self, feed, offset=0):
        # 每條來源獨立重連；只有收到有效資料的穩定連線才重設退避次數。
        await asyncio.sleep(offset)
        state = self.stat(feed)
        attempt = route = 0
        while True:
            session = uuid.uuid4().hex
            ctx = self.context(feed)
            activity = {'stable': False}
            endpoint = feed['urls'][route % len(feed['urls'])]
            state.update(attempts=state['attempts'] + 1, endpoint=endpoint, subscribed=False,
                         last_valid_us=None, last_valid_mono=None)
            try:
                await self.ws_budgets[feed['exchange']].acquire(1)
                LOG.info('event=connect_attempt feed=%s attempt=%s endpoint=%s', key(feed), state['attempts'], endpoint)
                async with self.http.ws_connect(endpoint, params=self.query(feed), proxy=self.proxy,
                        heartbeat=20, autoping=True, max_msg_size=8 * 1024 * 1024,
                        timeout=websocket_timeout()) as ws:
                    await self.receive_session(ws, feed, session, ctx, endpoint, activity)
            # Python 3.10 的 asyncio 逾時與內建 TimeoutError 是不同例外。
            except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError, RetryFeed, ProtocolError) as exc:
                state['last_error'] = reason(exc)
                state['unavailable_since_us'] = state['unavailable_since_us'] or time.time_ns() // 1000
                self.event(feed, 'disconnected', session, reason=reason(exc), endpoint=endpoint)
                route += 1
                attempt = 1 if activity['stable'] else attempt + 1
            finally:
                state.update(connected=False)
                self.event(feed, 'session_end', session)
            delay = random.uniform(.5, min(60, 2 ** min(attempt, 6)))
            LOG.info('event=reconnect_scheduled feed=%s delay_s=%.2f', key(feed), delay)
            await asyncio.sleep(delay)

    def check_session(self, feed, timing, now):
        # 區分連線輪換、訂閱逾時與資料靜默；沒有清算事件不視為故障。
        if now - timing['connected'] >= self.config['ws_max_age']:
            raise RetryFeed('scheduled_session_rotation')
        if not timing['ack'] and now - timing['connected'] > 10:
            raise RetryFeed('subscription_ack_timeout')
        limit = self.stale_limit(feed)
        if limit and now - timing['last_valid'] > limit:
            raise RetryFeed('market_data_stale')

    async def receive_session(self, ws, feed, session, ctx, endpoint, activity):
        # 訂閱後同時等待 WS 與 REST 快照；離開時取消所有本次連線的任務。
        now = time.monotonic()
        timing = dict(connected=now, last_valid=now, last_heartbeat=now, ack=not self.requires_ack)
        self.stat(feed)['connected'] = True
        self.event(feed, 'connected', session, endpoint=endpoint)
        await ws.send_json(self.subscription(feed))
        receive = asyncio.create_task(ws.receive())
        try:
            while True:
                now = time.monotonic()
                self.check_session(feed, timing, now)
                if self.application_heartbeat and now - timing['last_heartbeat'] >= 20:
                    await ws.send_str(self.application_heartbeat)
                    timing['last_heartbeat'] = now
                if timing['ack']:
                    await self.maintenance(feed, session, ctx, now)
                done, _ = await asyncio.wait([receive] + self.tasks(ctx), timeout=.5, return_when=asyncio.FIRST_COMPLETED)
                self.completed(feed, session, ctx, done)
                if receive not in done:
                    continue
                message, received = receive.result(), time.time_ns() // 1000
                if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    raise RetryFeed(f'websocket_closed code={ws.close_code} error={ws.exception()}')
                receive = asyncio.create_task(ws.receive())
                kind = self.receive_message(message, received, feed, session, ctx, endpoint)
                if kind == 'ack':
                    timing['ack'] = True
                if kind == 'data':
                    timing['last_valid'] = time.monotonic()
                    activity['stable'] = now - timing['connected'] > 60
        finally:
            pending = [receive] + self.tasks(ctx)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    def receive_message(self, message, received, feed, session, ctx, endpoint):
        # 先保存完整原文才解析；格式錯誤與未知訊息也保留，供日後重播診斷。
        if message.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
            return None
        binary = isinstance(message.data, bytes)
        self.emit(feed, 'ws', session, raw=base64.b64encode(message.data).decode('ascii') if binary else message.data,
                  encoding='base64' if binary else 'utf8', endpoint=endpoint, local_timestamp=received)
        try:
            obj = 'pong' if message.data in ('pong', b'pong') else json.loads(message.data)
            kind, data = self.classify(feed, obj)
            if kind == 'error':
                raise ProtocolError(str(obj))
            if kind == 'ack':
                self.stat(feed)['subscribed'] = True
                self.event(feed, 'subscribed', session)
            if kind == 'data':
                self.on_data(feed, session, ctx, data, received)
        except (ValueError, TypeError, KeyError) as exc:
            self.event(feed, 'protocol_error', session, reason=reason(exc))
            raise RetryFeed('invalid_ws_payload') from exc
        if kind == 'data':
            if not self.requires_ack:
                self.stat(feed)['subscribed'] = True
            self.valid(feed, received)
        return kind
    def health(self):
        # 只判定來源接收健康；訂單簿接合與完整性由 02 重播原始資料驗證。
        now=time.monotonic(); items={}; issues=[]
        for k,s in self.stats.items():
            market,symbol,ch=k.split('/'); age=now-s['last_valid_mono'] if s['last_valid_mono'] else None
            good = self.source_healthy(market, ch, s, age)
            items[k]={a:b for a,b in s.items() if a!='last_valid_mono'}|{'last_valid_age_s':age,'healthy':good}
            if not good: issues.append(k)
        return {'updated_us':time.time_ns()//1000,'pid':os.getpid(),'elapsed_s':now-self.start,'capability':self.capability,
                'data_quality':'healthy' if not issues else 'degraded','health_scope':'source_capture',
                'order_book_validation':'offline_only','unhealthy':issues,'feeds':items,'events':dict(self.events),
                'queue_size':self.writer.queue.qsize(),'queue_high_water':self.writer.high_water,
                'raw_enqueued':self.writer.seq,'raw_written':self.writer.written,'channel_csv_rows':self.writer.channel_rows}
    async def heartbeat(self):
        # 定期更新 status.json 並寫運行摘要；writer 故障或卡住時中止採集。
        from .raw import StorageError
        previous_received=0; previous_time=self.start; previous_unhealthy=None
        while True:
            if self.writer.error: raise StorageError(str(self.writer.error))
            if self.writer.queue.qsize() and time.monotonic()-self.writer.last_write>30: raise StorageError('writer stalled')
            status=self.health()
            await asyncio.to_thread(atomic_json,self.root/'status.json',status)
            now=time.monotonic(); received=sum(s['received'] for s in status['feeds'].values())
            rate=(received-previous_received)/max(now-previous_time,.001)
            # running 僅表示主循環存活；quality 另外判定來源是否正常，不能混為一談。
            level=logging.INFO if status['data_quality']=='healthy' else logging.WARNING
            LOG.log(level, 'event=heartbeat process=running quality=%s uptime_s=%.1f healthy_feeds=%s/%s received=%s rate_per_s=%.2f raw_written=%s raw_pending=%s csv_rows=%s queue=%s',
                    status['data_quality'],status['elapsed_s'],len(status['feeds'])-len(status['unhealthy']),len(status['feeds']),
                    received,rate,self.writer.written,max(0,self.writer.seq-self.writer.written),self.writer.channel_rows,self.writer.queue.qsize())
            current=tuple(status['unhealthy'])
            if current!=previous_unhealthy:
                LOG.log(level,'event=health_changed unhealthy_feeds=%s',list(current))
            previous_received=received;previous_time=now;previous_unhealthy=current
            await asyncio.sleep(self.config['heartbeat_seconds'])
