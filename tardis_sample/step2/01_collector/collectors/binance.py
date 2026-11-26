"""Binance spot／USD-M 原始採集：來源、訂閱、REST 排程及序號觀察，不重建訂單簿。"""
import asyncio
import hashlib
import json
import math
import time
import uuid
import aiohttp
from common import LOG, ProtocolError
from common.binance import classify, validate
from ..transport import (BaseCollector, Budget, METADATA,
                    HTTPFailure, retry_after, reason, key)


SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'GRTUSDT']
FIELDS = {
    'trade': ('e', 'E', 's', 't', 'p', 'q', 'b', 'a', 'T', 'm', 'M', 'X'),
    'bookTicker': ('e', 'u', 'E', 'T', 's', 'b', 'B', 'a', 'A'),
    'depth@100ms': ('e', 'E', 'T', 's', 'U', 'u', 'pu', 'b', 'a'),
    'depth@0ms': ('e', 'E', 'T', 's', 'U', 'u', 'pu', 'b', 'a'),
    'depthSnapshot': ('lastUpdateId', 'E', 'T', 'bids', 'asks'),
    'ticker': ('e', 'E', 's', 'p', 'P', 'w', 'c', 'Q', 'o', 'h', 'l', 'v', 'q', 'O', 'C', 'F', 'L', 'n'),
    'markPrice@1s': ('e', 'E', 's', 'p', 'i', 'P', 'r', 'T'),
    'openInterest': ('openInterest', 'symbol', 'time'),
    'forceOrder': ('e', 'E', 'o'),
}


class RestPolicy:
    """Binance REST 的路由、回應欄位、權重及配額分配。"""
    channels = ('depthSnapshot', 'openInterest')
    snapshot = 'depthSnapshot'
    metadata_channel = 'exchangeInfo'
    metadata_interval = 3600
    regular_percent = 70
    snapshot_percent = 20
    usable_percent = regular_percent + snapshot_percent
    used_weight_header = 'x-mbx-used-weight-1m'
    weight_kind = 'REQUEST_WEIGHT'
    throttle_statuses = (418, 429)
    time_unit = 'ms'

    @staticmethod
    def cost(market, channel):
        # 一千檔深度在現貨與期貨使用不同權重。
        if channel == 'depthSnapshot':
            return 50 if market == 'binance' else 20
        return 20 if market == 'binance' and channel == 'exchangeInfo' else 1

    @staticmethod
    def request(config, market, symbol, channel):
        # 共用兩種採集模式的原生端點與深度限制。
        params = {'symbol': symbol} if symbol else {}
        if channel == 'depthSnapshot':
            params['limit'] = 1000
        path = 'depth' if channel == 'depthSnapshot' else channel
        return config['rest'][market] + '/' + path, params

    @staticmethod
    def dependency(market, channel):
        # 快照須等相應的增量來源訂閱並收訊。
        if channel == 'depthSnapshot':
            return 'depth@100ms' if market == 'binance' else 'depth@0ms'
        return None

    @staticmethod
    def requested_interval(config, channel):
        # 採集設定分別指定未平倉量及快照的期望週期。
        return config['oi_interval'] if channel == 'openInterest' else config['snapshot_interval']

    @classmethod
    def interval(cls, config, market, channel, capacity):
        # 兩種來源同時啟用時，在例行預算內按六比四分配權重。
        settings = config['market_settings'][market]
        both = set(cls.channels).issubset(settings['channels'])
        share = (.6 if channel == 'openInterest' else .4) if both else 1
        unit = 5 if channel == 'openInterest' else 60
        per_minute = len(settings['symbols']) * cls.cost(market, channel)
        seconds = per_minute * 60 / (capacity * (cls.regular_percent / 100) * share)
        return max(cls.requested_interval(config, channel), math.ceil(seconds / unit) * unit)

    @staticmethod
    def rate_limits(data, symbols, configured_cap):
        # 先核對市場商品，再解析交易所聲明的權重與請求數視窗。
        missing = set(symbols) - {x['symbol'] for x in data['symbols']}
        if missing:
            raise ValueError(f'unknown symbols: {sorted(missing)}')
        limits = [int(r['limit']) / int(r['intervalNum']) for r in data['rateLimits']
                  if r['rateLimitType'] == 'REQUEST_WEIGHT' and r['interval'] == 'MINUTE']
        if not limits:
            raise ValueError('exchangeInfo missing minute weight limit')
        units = {'SECOND': 1, 'MINUTE': 60, 'HOUR': 3600, 'DAY': 86400}
        windows = [(r['rateLimitType'], units[r['interval']] * int(r['intervalNum']), int(r['limit']))
                   for r in data['rateLimits'] if r['rateLimitType'] in ('REQUEST_WEIGHT', 'RAW_REQUESTS')
                   and r['interval'] in units]
        return min(int(min(limits)), configured_cap), windows


class Collector(BaseCollector):
    """交易所專屬的來源與協議實作；傳輸、落盤與日誌沿用 BaseCollector。"""
    rest_policy = RestPolicy
    legacy_multi_market = True
    optional_channels = ('forceOrder',)
    probe_channels = ('trade', 'bookTicker', 'depth@0ms', 'depth@100ms', 'ticker', 'markPrice@1s')
    native_fields = FIELDS
    variant_fields = {('binance', 'depth@100ms'): ({'E'}, 'publication_timestamp_variant')}
    validate_event = staticmethod(validate)

    def __init__(self, config, root, writer, http):
        # 單路採集的 REST 權重按市場計算；雙副本模式使用持久化排程。
        super().__init__(config, root, writer, http)
        self.budgets = {m: Budget(config['rest_weight_per_minute'].get(m, 1200)) for m in config['markets']}
        self.rest_slots = asyncio.Semaphore(6)

    @staticmethod
    def legacy_symbols(config, market):
        # 舊版單組設定以頂層清單指定現貨及期貨商品。
        return config['symbols']

    def polling_tasks(self, config):
        # 舊版採集模式為每個期貨商品啟動一個未平倉量輪詢。
        tasks = []
        if 'binance-futures' in config['markets']:
            for symbol in config['symbols']:
                feed = dict(exchange='binance-futures', symbol=symbol, channel='openInterest', time_unit='ms')
                self.stat(feed)
                tasks.append(asyncio.create_task(self.oi(feed)))
        return tasks

    def stale_limit(self, feed):
        # 清算沒有成交時可保持靜默，深度頻道共用深度門檻。
        channel = feed['channel']
        if channel in self.optional_channels:
            return None
        return self.config['stale_seconds'].get('depth' if channel.startswith('depth@') else channel)

    def source_healthy(self, market, channel, state, age):
        # REST 來源以最近一次有效回應及各自的週期判定。
        if channel == 'openInterest':
            return state['connected'] and age is not None and age <= self.config['stale_seconds'][channel]
        if channel == 'depthSnapshot':
            return age is not None and age <= self.config['snapshot_interval'] + self.config['rest_timeout']
        return super().source_healthy(market, channel, state, age)

    capability = 'binance_full_sources'
    connection_spacing = .04
    # 僅列出具可靠原生事件身分的來源；身分不等於連續性保證。
    replica_strong_channels = ('trade', 'bookTicker', 'depth@100ms', 'depth@0ms')
    continuity_channels = {'depth@100ms': 'U/u range', 'depth@0ms': 'pu/u predecessor'}
    replica_silence_seconds = {'ticker': 15, 'markPrice@1s': 10}
    connection_limits = ((300, 300), (1, 5))
    @staticmethod
    def default_symbols(market): 
        # 回傳該市場的原生商品代碼，避免把另一交易所的代碼直接套入。
        return SYMBOLS
    @staticmethod
    def configure(config, market):
        # 用本地來源 JSON 驗證 Binance 必要 channel 與頻率，保存來源雜湊供追溯。
        if not config.get('managed'):
            for name in ('snapshot_interval', 'oi_interval', 'rest_timeout'):
                if config[name] <= 0:
                    raise ValueError(f'{name} must be positive')
        if not config.get('managed') and not set(config['symbols']) <= set(SYMBOLS): raise ValueError('Binance supports BTCUSDT/ETHUSDT/SOLUSDT/GRTUSDT')
        path = METADATA / (market + '.json'); data = json.loads(path.read_text())
        details = {c['name']: c for c in data['channelDetails']}
        names = ['trade', 'bookTicker', 'depth', 'depthSnapshot']
        if market == 'binance-futures': names += ['ticker', 'markPrice', 'forceOrder', 'openInterest']
        for name in names:
            if not details[name].get('sourceFor'): raise ValueError(f'missing sourceFor: {market}/{name}')
        if market == 'binance' and details['depth']['frequency'] != '100ms': raise ValueError('spot depth metadata changed')
        if market == 'binance-futures' and 'depth@0ms' not in details['depth']['additionalInfo']: raise ValueError('futures depth metadata changed')
        config['metadata'][market] = {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
            'source_path': str(path), 'channels': [details[n] for n in names]}
    @staticmethod
    def channels(market):
        # 列出該市場的原生來源；現貨與衍生品清單明確分開。
        names = ['trade', 'bookTicker', 'depth@100ms' if market == 'binance' else 'depth@0ms', 'depthSnapshot']
        return names + (['ticker', 'markPrice@1s', 'openInterest', 'forceOrder'] if market == 'binance-futures' else [])
    @classmethod
    def feeds(cls, config, market):
        # 展開每個商品／channel 的端點與時間單位，每個來源各自建立連線。
        result = []
        for symbol in config['symbols']:
            for channel in cls.channels(market):
                if channel in ('depthSnapshot', 'openInterest'): continue
                route = 'trade' if channel == 'trade' else ('public' if channel.startswith('depth') or channel == 'bookTicker' else 'market')
                result.append(dict(exchange=market, symbol=symbol, channel=channel,
                    urls=config['spot_ws'] if market == 'binance' else config['futures_ws'][route],
                    time_unit='us' if market == 'binance' else 'ms'))
        return result
    classify = staticmethod(classify)
    def query(self, feed): 
        # Binance spot 要求微秒事件時間；futures 保留原生毫秒。
        return {'timeUnit': 'microsecond'} if feed['time_unit'] == 'us' else {}
    def context(self, feed):
        # 每條連線只記 REST 排程與最後序號，不保存價位或合併行情欄位。
        depth = feed['channel'].startswith('depth@')
        if depth:
            self.stat(dict(feed, channel='depthSnapshot'))
        return {'depth': depth, 'candidate': None, 'generation': 0, 'last_snapshot': None,
                'next_snapshot': 0, 'failures': 0, 'last_depth_u': None, 'snapshot_needed': True}
    def tasks(self, ctx): 
        # 回傳正在取得的 snapshot 任務，讓共用循環同時等待 WS 與 REST。
        return [ctx['candidate']] if ctx['candidate'] else []
    async def maintenance(self, feed, session, ctx, now):
        # 初始、定期或觀察到序號缺口時補取快照；原始 depth 始終獨立保存。
        if not ctx['depth'] or ctx['candidate'] or now < ctx['next_snapshot']: return
        needs = ctx['snapshot_needed'] or ctx['last_snapshot'] is None
        if ctx['last_snapshot'] is not None:
            needs = needs or now - ctx['last_snapshot'] >= self.config['snapshot_interval']
        if needs:
            # 此事件只標記請求邊界；緩存與接合全部由 02 離線重播處理。
            ctx['generation'] += 1; ctx['snapshot_needed'] = False
            self.event(feed, 'snapshot_begin', session, generation=ctx['generation'])
            ctx['candidate'] = asyncio.create_task(self.rest(feed, 'depthSnapshot', session, ctx['generation']))
    def completed(self, feed, session, ctx, done):
        # 只確認 REST 回應已保存；不把成功取得快照當成訂單簿已同步。
        task = ctx['candidate']
        if not task or task not in done: return
        try:
            task.result()
            ctx['last_snapshot'] = time.monotonic(); ctx['failures'] = 0
            ctx['next_snapshot'] = ctx['last_snapshot'] + min(2, self.config['snapshot_interval'])
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError, HTTPFailure, ProtocolError) as exc:
            ctx['snapshot_needed'] = True; ctx['failures'] += 1
            ctx['next_snapshot'] = time.monotonic() + min(30, 2 ** min(ctx['failures'], 5))
            self.event(feed, 'snapshot_error', session, reason=reason(exc), generation=ctx['generation'])
        ctx['candidate'] = None
    def on_data(self, feed, session, ctx, data, received):
        # 原文已入列；格式檢查與序號觀察不改寫、合併或去重任何訊息。
        validate(data, feed['channel'], feed['symbol'])
        if not ctx['depth']: return
        last, current = ctx['last_depth_u'], int(data['u'])
        previous = int(data['pu']) if feed['exchange'] == 'binance-futures' else None
        # 重複／過期訊息仍保存在 raw；只避免讓觀察用的最後序號倒退。
        if last is not None and current <= last: return
        gap = last is not None and (previous != last if previous is not None else int(data['U']) > last + 1)
        if gap:
            ctx['snapshot_needed'] = True
            self.event(feed, 'depth_sequence_gap', session, previous_u=last,
                       U=int(data['U']), u=current, pu=previous)
        ctx['last_depth_u'] = current
    async def rest(self,feed,channel,session,generation=None):
        # 按權重與併發上限請求 snapshot/OI，保留回應、處理限流並驗證成功資料。
        market,symbol=feed['exchange'],feed['symbol'];budget=self.budgets[market]
        snapshot = channel == RestPolicy.snapshot
        await budget.acquire(RestPolicy.cost(market, channel))
        endpoint, params = RestPolicy.request(self.config, market, symbol, channel)
        started=time.time_ns()//1000
        async with self.rest_slots:
            async with self.http.get(endpoint,params=params,proxy=self.proxy,
                    timeout=aiohttp.ClientTimeout(total=self.config['rest_timeout'],connect=5)) as response:
                raw=await response.text();received=time.time_ns()//1000
                headers={k:v for k,v in response.headers.items() if k.lower().startswith('x-mbx-used-weight') or k.lower()=='retry-after'}
                rfeed=dict(feed,channel=channel,time_unit='ms')
                if response.status!=200:
                    if response.status in (418,429):
                        # 同市場的 REST 請求共用冷卻期限，避免其他商品持續撞限流。
                        pause=retry_after(response.headers.get('Retry-After'))
                        budget.cooldown=max(budget.cooldown,time.monotonic()+pause)
                        LOG.warning('event=rest_rate_limited feed=%s status=%s cooldown_s=%.2f',key(rfeed),response.status,pause)
                    self.emit(rfeed,'rest_error',session,raw=raw,endpoint=endpoint,local_timestamp=received,
                              status=response.status,headers=headers,request_started_us=started)
                    raise HTTPFailure(f'HTTP {response.status}: {raw[:200]}')
                try:data=json.loads(raw);validate(data,channel,symbol)
                except (ValueError,TypeError) as e:
                    self.emit(rfeed,'rest_error',session,raw=raw,endpoint=endpoint,local_timestamp=received,
                              status=response.status,reason=reason(e),request_started_us=started)
                    raise ProtocolError(f'invalid REST {channel}') from e
                record=self.emit(rfeed,'rest',session,raw=raw,generated=True,generation=generation,
                                 endpoint=endpoint,local_timestamp=received,headers=headers,request_started_us=started)
                if snapshot:LOG.info('event=snapshot_received feed=%s generation=%s elapsed_ms=%.1f',key(rfeed),generation,(received-started)/1000)
                self.valid(rfeed,received);return data,record
    async def oi(self,feed):
        # 以單調時鐘排程 OI 輪詢，單條 poller 不重疊請求，失敗時退避。
        session=uuid.uuid4().hex;deadline=time.monotonic();attempt=0
        while True:
            await asyncio.sleep(max(0,deadline-time.monotonic()))
            try:
                await self.rest(feed,'openInterest',session);attempt=0;self.stat(feed)['connected']=True
            except (aiohttp.ClientError,asyncio.TimeoutError,TimeoutError,HTTPFailure,ProtocolError) as e:
                attempt+=1;s=self.stat(feed);s['connected']=False;s['last_error']=reason(e)
                s['unavailable_since_us']=s['unavailable_since_us'] or time.time_ns()//1000
                self.event(feed,'rest_error',session,reason=reason(e))
            # 必須等上一個請求結束才排下一次；逾時時不補發重疊請求。
            deadline=max(deadline+self.config['oi_interval'],time.monotonic()+min(60,2**min(attempt,6)) if attempt else time.monotonic())

    @staticmethod
    def replica_subscription(feeds):
        # 合併同一路由／channel 的商品訂閱，不更換原生 channel。
        return {'method': 'SUBSCRIBE', 'params': [f'{f["symbol"].lower()}@{f["channel"]}' for f in feeds], 'id': 1}

    @staticmethod
    def replica_symbol(obj, channel):
        # combined stream 的商品位於 data；清算另包在 o。
        if not isinstance(obj, dict): return None
        data = obj.get('data', {})
        if channel == 'forceOrder': data = data.get('o', {})
        return data.get('s')

    @staticmethod
    def replica_identity(channel, data, envelope):
        # 成交與深度採原生 ID；時間事件使用跨副本配對，避免誤刪合法重複。
        from ..replica_protocol import stream_marker
        if channel == 'trade': return ('trade', data['t']), True, None, False
        if channel == 'bookTicker': return ('book', data['u']), True, None, False
        if channel.startswith('depth@'):
            mark = stream_marker(channel, data['u'], data.get('pu'), data['U'])
            return ('depth', data['U'], data['u'], data.get('pu')), True, mark, False
        if channel == 'forceOrder':
            return ('liquidation', data.get('E'), data['o']['T']), False, None, False
        return (channel, data['E']), False, None, False
