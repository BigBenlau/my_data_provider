"""管線回歸測試：固定資料與本機伺服器驗證原始保存、三階段語義、異常恢復及日誌。"""
import asyncio
import base64
from collections import Counter
import csv
import fcntl
from functools import partial
import gzip
import hashlib
import importlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common import ProtocolError, SCHEMAS, atomic_json, dec, utc
from common.protocol import validate
from common.raw_reader import read_records

transport = importlib.import_module('01_collector.transport')
configuration, feeds, Budget = transport.configuration, transport.feeds, transport.Budget
raw_writer = importlib.import_module('01_collector.raw')
Writer, StorageError, ChannelCSV = raw_writer.Writer, raw_writer.StorageError, raw_writer.ChannelCSV
CHANNELS, channel_row, export_channels = raw_writer.CHANNELS, raw_writer.channel_row, raw_writer.export_channels
Collector = importlib.import_module("01_collector.collectors.binance").Collector
# 數字開頭的入口檔使用 importlib 載入，避免為測試另留重複入口。
collect_entry = importlib.import_module('01_collector.01_collect')
normalize_entry = importlib.import_module('02_normalize_tardis.mappers.binance')
replay = importlib.import_module('02_normalize_tardis.normalization').replay
compare_entry = importlib.import_module('03_compare.03_compare_tardis')
Mapper, compare = normalize_entry.Mapper, compare_entry.compare
process = partial(replay, Mapper)
Book, BookEngine, BookError = normalize_entry.Book, normalize_entry.BookEngine, normalize_entry.BookError

NOW=1792600000000000

async def wait_until(predicate):
    # 等待可觀察條件；由呼叫端的 wait_for 設定逾時，相容 Python 3.10。
    while not predicate():
        await asyncio.sleep(.01)

def snapshot(n=100):
    # 建立固定深度快照，讓序號接合與重播結果可重複。
    return {'lastUpdateId':n,'bids':[['100','2'],['90','4']],'asks':[['101','3'],['110','5']]}
def delta(u=101,pu=100,U=None):
    # 建立可控制 U/u/pu 的增量，涵蓋正常更新與缺口。
    return {'s':'BTCUSDT','U':u if U is None else U,'u':u,'pu':pu,'E':NOW//1000,'b':[['100','4']],'a':[]}
def event(ch,data,market='binance-futures',symbol='BTCUSDT',session='s',**extra):
    # 把固定 payload 包成與實際採集一致的 envelope。
    rest=ch in ('depthSnapshot','openInterest')
    return {'kind':'rest' if rest else 'ws','exchange':market,'symbol':symbol,'channel':ch,'session':session,
            'time_unit':'ms','local_timestamp':NOW,'raw':json.dumps(data if rest else {'stream':f'{symbol.lower()}@{ch}','data':data}),**extra}

class ConfigTests(unittest.TestCase):
    """ConfigTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def test_36_feeds_from_metadata(self):
        # 確認 Binance 四商品共 36 條 WS，來源頻率與時間單位未被替換。
        c=configuration();f=feeds(c)
        self.assertEqual(len(f),36);self.assertEqual(len({(x['exchange'],x['symbol']) for x in f}),8)
        self.assertEqual({x['time_unit'] for x in f if x['exchange']=='binance'},{'us'})
        self.assertIn('depth@0ms',{x['channel'] for x in f});self.assertNotIn('aggTrade',{x['channel'] for x in f})
    def test_validate_bad_data(self):
        # 錯誤商品、非有限數值與非 JSON payload 不得被當成有效資料。
        for data in ({'symbol':'BTCUSDT','openInterest':'NaN'}, {'symbol':'ETHUSDT','openInterest':'2'}, '<html>'):
            with self.assertRaises(ProtocolError):validate(data,'openInterest','BTCUSDT')

class BookTests(unittest.TestCase):
    """BookTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def test_spot_boundary_and_futures_pu(self):
        # 分別檢查 spot U/u 與 futures pu/u 接合、重複增量及序號缺口。
        for market in ('binance','binance-futures'):
            b=Book(market);self.assertIsNone(b.snapshot(snapshot(),NOW,'ms'))
            q=b.update(delta(),NOW,'ms');self.assertEqual(q['bid_amount'],dec(4))
            self.assertIsNone(b.update(delta(),NOW,'ms'))
            with self.assertRaises(BookError):b.update(delta(103,102),NOW,'ms')
    def test_spot_microseconds(self):
        # 確認 spot 微秒事件時間不會被再次乘上一千。
        b=Book('binance');b.snapshot(snapshot(),NOW,'ms');d=delta();d['E']=NOW+1
        self.assertEqual(b.update(d,NOW+2,'us')['timestamp'],NOW+1)
    def test_buffered_snapshot_and_deletion(self):
        # 確認快照前的 delta 會重播，且數量零會刪除價位。
        b=Book('binance-futures');b.update(delta(),NOW,'ms');b.snapshot(snapshot(),NOW,'ms')
        d=delta(102,101);d['b']=[['100','0']]
        self.assertEqual(b.update(d,NOW,'ms')['bid_price'],dec(90))
    def test_live_generation_swap_without_disconnect(self):
        # 候選快照接合完成才切換，更新期間保留有效舊 book。
        e=BookEngine('binance-futures');e.begin(1);e.update(delta(),NOW,'ms');self.assertIsNotNone(e.snapshot(snapshot(),NOW,'ms',1))
        original=e.current;e.begin(2);e.update(delta(102,101),NOW,'ms')
        self.assertIs(e.current,original)
        self.assertIsNotNone(e.snapshot(snapshot(101),NOW,'ms',2));self.assertIsNot(e.current,original)
        self.assertIsNone(e.pending);self.assertEqual(e.current.last,102)
    def test_bad_candidate_preserves_current(self):
        # 候選快照不相容時，已同步 book 不應一起失效。
        e=BookEngine('binance-futures');e.begin(1);e.snapshot(snapshot(),NOW,'ms',1);e.update(delta(),NOW,'ms')
        original=e.current;e.begin(2);e.update(delta(102,101),NOW,'ms')
        with self.assertRaises(BookError):e.snapshot(snapshot(20),NOW,'ms',2)
        self.assertIs(e.current,original)
    def test_bound_and_overflow(self):
        # 超出緩存與已知快照範圍必須明確報錯。
        b=Book('binance',limit=1);b.update(delta(),NOW,'ms')
        with self.assertRaises(BookError):b.update(delta(102),NOW,'ms')
        b=Book('binance');b.snapshot(snapshot(),NOW,'ms');d=delta();d['b']=[['100','0'],['90','0'],['80','1']]
        with self.assertRaises(BookError):b.update(d,NOW,'ms')

class MappingTests(unittest.TestCase):
    """MappingTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def setUp(self):
        # 每個測試建立獨立 mapper 與記憶體資料庫，避免狀態互相污染。
        self.db=sqlite3.connect(':memory:');self.m=Mapper(self.db);self.addCleanup(self.db.close)
    def test_symbols_and_markets_dedup(self):
        # 交易 ID 去重必須以市場與商品區隔，不能跨市場誤刪。
        for market in ('binance','binance-futures'):
            for s in ('BTCUSDT','ETHUSDT'):
                d={'s':s,'t':123,'T':NOW//1000,'p':'100.1','q':'.01','m':True}
                r=event('trade',d,market,s);self.assertEqual(len(self.m.consume(r)),1)
                self.assertEqual(self.m.consume(r),[])
        self.assertEqual(self.m.issues['duplicate_trades'],4)
    def test_offbook_liquidation_and_ticker(self):
        # 驗證 off-book、清算 p/z、OI 合併及不變 ticker 的輸出規則。
        d={'s':'BTCUSDT','t':1,'T':NOW//1000,'p':'100','q':'2','m':False,'X':'ADL'}
        self.assertEqual(self.m.consume(event('trade',d)),[])
        d={'o':{'s':'BTCUSDT','T':NOW//1000,'S':'SELL','X':'FILLED','p':'90','ap':'95','q':'9','z':'3'}}
        row=self.m.consume(event('forceOrder',d))[0][1];self.assertEqual(row['amount'],dec(3));self.assertEqual(row['price'],dec(90))
        mark={'s':'BTCUSDT','E':NOW//1000,'T':NOW//1000+3000,'p':'100','i':'101','r':'.001'}
        self.m.consume(event('markPrice@1s',mark))
        r=self.m.consume(event('openInterest',{'symbol':'BTCUSDT','openInterest':'9','time':NOW//1000+1}))[0][1]
        self.assertEqual(r['timestamp'],NOW);self.assertNotIn('predicted_funding_rate',r)
        self.assertEqual(self.m.consume(event('markPrice@1s',mark)),[])
    def test_spot_bbo_fallback_and_session_reset(self):
        # 驗證 spot 時間回退、binary 原文解析以及斷線後重建狀態。
        r=event('bookTicker',{'s':'BTCUSDT','u':1,'a':'101','A':'2','b':'100','B':'3'},'binance')
        self.assertEqual(self.m.consume(r)[0][1]['timestamp'],NOW)
        import base64
        binary=dict(r,raw=base64.b64encode(r['raw'].encode()).decode(),encoding='base64')
        self.assertEqual(self.m.consume(binary)[0][1]['timestamp'],NOW)
        self.assertEqual(self.m.consume(dict(binary,raw='/w==')),[])
        begin={'kind':'event','event':'snapshot_begin','exchange':'binance','symbol':'BTCUSDT','channel':'depth@100ms','session':'s','generation':1,'local_timestamp':NOW}
        self.m.consume(begin);self.m.consume(event('depth@100ms',delta(),'binance'))
        self.assertEqual(len(self.m.consume(event('depthSnapshot',snapshot(),'binance',generation=1))),1)
        self.m.consume(dict(begin,event='session_end'))
        self.assertEqual(self.m.consume(event('depth@100ms',delta(102,101),'binance',session='new')),[])

    def test_upstream_snapshot_buffer_timestamp_and_gap(self):
        # 核對上游先套緩存再輸出快照、沿用快照時間；缺口停輸屬本地嚴格規則。
        begin={'kind':'event','event':'snapshot_begin','exchange':'binance-futures','symbol':'BTCUSDT',
               'channel':'depth@0ms','session':'s','generation':1,'local_timestamp':NOW}
        self.m.consume(begin)
        self.assertEqual(self.m.consume(event('depth@0ms',delta())),[])
        self.assertEqual(self.m.consume(event('depth@0ms',dict(delta(102,101),b=[['100','6']]))),[])
        rows=self.m.consume(event('depthSnapshot',dict(snapshot(),T=NOW//1000-20),generation=1))
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0][1]['bid_amount'],dec('6'))
        self.assertEqual(rows[0][1]['timestamp'],NOW-20000)
        self.assertEqual(rows[0][1]['local_timestamp'],NOW)
        # 深層更新不改最佳價；缺口後即使收到下一筆也不能沿用失效訂單簿。
        self.assertEqual(self.m.consume(event('depth@0ms',dict(delta(103,102),b=[['90','9']]))),[])
        self.assertEqual(self.m.consume(event('depth@0ms',delta(105,104))),[])
        self.assertEqual(self.m.consume(event('depth@0ms',delta(106,105))),[])
        self.assertEqual(self.m.issues['sequence_gap'],1)
        self.m.consume(dict(begin,generation=2))
        self.m.consume(event('depth@0ms',delta(107,106)))
        self.assertEqual(len(self.m.consume(event('depthSnapshot',snapshot(106),generation=2))),1)

    def test_upstream_ticker_change_and_timestamp_rules(self):
        # 核對 helper：相同數值不輸出，較舊事件不倒退時間，OI 沿用已知時間。
        mark={'s':'BTCUSDT','E':NOW//1000,'T':0,'p':'100','r':''}
        row=self.m.consume(event('markPrice@1s',mark))[0][1]
        self.assertNotIn('funding_rate',row);self.assertNotIn('index_price',row)
        self.assertEqual(self.m.consume(event('markPrice@1s',dict(mark,E=NOW//1000+10))),[])
        row=self.m.consume(event('ticker',{'s':'BTCUSDT','E':NOW//1000-10,'c':'101'}))[0][1]
        self.assertEqual(row['timestamp'],NOW+10000)
        row=self.m.consume(event('openInterest',{'symbol':'BTCUSDT','openInterest':'9'}))[0][1]
        self.assertEqual(row['timestamp'],NOW+10000)
        self.assertEqual(row['last_price'],dec('101'));self.assertEqual(row['mark_price'],dec('100'))
        self.assertNotIn('predicted_funding_rate',row)

    def test_json_numeric_price_keeps_decimal_precision(self):
        # 原文若以 JSON 數值傳送價格，也必須保留 Decimal 精度而非先轉浮點。
        record=event('trade',{'s':'BTCUSDT','t':9,'T':NOW//1000,'p':'PRICE','q':'2','m':False})
        record['raw']=record['raw'].replace('"PRICE"','100.1234567890123456789')
        row=self.m.consume(record)[0][1]
        self.assertEqual(row['price'],dec('100.1234567890123456789'))

class StorageTests(unittest.TestCase):
    """StorageTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def test_rotation_and_recovery(self):
        # 驗證切檔、時鐘倒退順序以及顯式 .part 恢復。
        with tempfile.TemporaryDirectory() as d:
            w=Writer(d,min_free_gib=.001,segment_seconds=.01)
            w.emit('event',local_timestamp=NOW,n=1);time.sleep(.05);w.emit('event',local_timestamp=NOW-1000,n=2);w.close()
            rs=list(read_records(d));self.assertEqual([r['seq'] for r in rs],[1,2]);self.assertEqual(len(list((Path(d)/'raw').glob('*.gz'))),2)
            p=next((Path(d)/'raw').glob('*.gz'));part=p.with_name(p.name+'.part');p.rename(part)
            with self.assertRaises(ValueError):list(read_records(d))
            self.assertEqual(len(list(read_records(d,True))),2)
            part.write_bytes(part.read_bytes()[:-8]);warnings=[];list(read_records(d,True,warnings));self.assertTrue(warnings)
    def test_disk_failure_surfaces(self):
        # 模擬磁碟不足，確認錯誤會傳到主程序而非靜默遺失。
        with tempfile.TemporaryDirectory() as d:
            w=Writer(d,min_free_gib=1e9);w.emit('event');time.sleep(.1)
            with self.assertRaises(StorageError):w.close()
    def test_replay_twice_and_compare_window(self):
        # 同一 raw 重播兩次應一致，對帳必須遵守明確時間窗。
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);run=root/'rawrun';run.mkdir();w=Writer(run,min_free_gib=.001)
            data={'s':'BTCUSDT','t':1,'T':NOW//1000,'p':'100.00','q':'1.00','m':False}
            r=event('trade',data);r.pop('local_timestamp');kind=r.pop('kind');w.emit(kind,local_timestamp=NOW,**r);w.close()
            atomic_json(run/'manifest.json',{'schema_version':2,'status':'stopped','started_us':NOW-10,'ended_us':NOW+10,'raw_records':1})
            a,b=root/'a',root/'b';process(run,a);process(run,b)
            for p in a.rglob('*.csv'):self.assertEqual(p.read_bytes(),(b/p.relative_to(a)).read_bytes())
            cmp=compare(a,b,root/'cmp',NOW-1,NOW+1)
            trade=next(x for x in cmp['results'] if x['type']=='trades');self.assertEqual(trade['shared_trade_ids'],1)
            self.assertEqual(trade['only_tardis_ids'],0)
            missing=compare(a,root/'none',root/'missing',NOW-1,NOW+1)
            self.assertTrue(all(x['assessment']=='missing_reference' for x in missing['results']))
            with self.assertRaises(FileExistsError):process(run,a)

class ComparisonTests(unittest.TestCase):
    """ComparisonTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def test_trade_content_missing_ids_duplicates_and_end_exclusive(self):
        # 驗證 trade 內容差異、各方獨有 ID、重複列與不包含 end 的時間窗。
        from common import utc
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            def write(where,items):
                # 建立指定 trade 測試列，控制對帳差異與時間窗邊界。
                path=root/where/'binance-futures/trades'/f'{utc(NOW)}_BTCUSDT.csv';path.parent.mkdir(parents=True)
                with path.open('w',newline='') as f:
                    w=csv.DictWriter(f,fieldnames=SCHEMAS['trades']);w.writeheader()
                    for tid,price,t in items:w.writerow(dict(exchange='binance-futures',symbol='BTCUSDT',timestamp=t,local_timestamp=t+1,id=tid,side='buy',price=price,amount='1'))
            write('a',[(1,'100.0',NOW),(2,'101',NOW),(4,'102',NOW+10)])
            write('b',[(1,'99',NOW),(3,'101',NOW),(3,'101',NOW)])
            r=compare(root/'a',root/'b',root/'out',NOW,NOW+10)['results'][0]
            self.assertEqual(r['shared_trade_ids'],1);self.assertEqual(r['ids_with_matching_content'],0)
            self.assertEqual(r['only_ours_ids'],1);self.assertEqual(r['only_tardis_ids'],1)
            self.assertEqual(r['tardis']['duplicate_identical_rows'],1);self.assertEqual(r['mismatch_id_examples'],['1'])
            self.assertEqual(r['ours']['rows'],2)

class AsyncTests(unittest.IsolatedAsyncioTestCase):
    """AsyncTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    async def asyncSetUp(self):
        # 建立獨立暫存目錄與 writer，供非同步測試共用。
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.writer=Writer(self.root,min_free_gib=.001)
    async def asyncTearDown(self):
        # 關閉 writer 並清除暫存資料，避免留下背景執行緒。
        self.writer.close();self.temp.cleanup()
    async def test_legacy_asyncio_timeout_reconnects(self):
        # 模擬 Python 3.10 的獨立逾時類別，確認來源會退避重連而非結束任務。
        from unittest.mock import Mock
        class LegacyTimeout(Exception):
            """獨立於內建 TimeoutError，重現 Python 3.10 的例外關係。"""
            pass
        http = Mock()
        http.ws_connect.side_effect = LegacyTimeout('fixture timeout')
        config = configuration()
        collector = Collector(config, self.root, self.writer, http)
        feed = feeds(config)[0]
        with patch.object(transport.asyncio, 'TimeoutError', LegacyTimeout), patch.object(transport.random, 'uniform', return_value=.01):
            task = asyncio.create_task(collector.stream(feed))
            try:
                await asyncio.wait_for(wait_until(lambda: collector.events['disconnected'] >= 2 or task.done()), 2)
                self.assertFalse(task.done())
                self.assertGreaterEqual(collector.events['disconnected'], 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    async def test_bad_rest_not_counted_success_and_429_cooldown(self):
        # HTTP 200 非法內容不算成功，429 必須套用 Retry-After 冷卻。
        class Response:
            """Response 測試組：集中驗證對應模組的行為與失敗邊界。"""
            status=200;headers={}
            async def text(self):
                # 回傳固定錯誤本文，模擬 HTTP 成功但內容無效。
                return '<html>bad</html>'
            async def __aenter__(self):
                # 模擬 aiohttp 非同步 response context，回傳固定回應物件。
                return self
            async def __aexit__(self,*_):
                # 結束假 response context，不吞掉測試中的例外。
                pass
        response=Response()
        class Http:
            """Http 測試組：集中驗證對應模組的行為與失敗邊界。"""
            def get(self,*a,**k):
                # 回傳假 HTTP 回應，測試不得因此連到外部網路。
                return response
        c=Collector(configuration(),self.root,self.writer,Http());feed={'exchange':'binance-futures','symbol':'BTCUSDT','channel':'openInterest','time_unit':'ms'}
        with self.assertRaises(ProtocolError):await c.rest(feed,'openInterest','s')
        self.assertEqual(c.stat(feed)['received'],0)
        response.status=429;response.headers={'Retry-After':'20'}
        HTTPFailure = transport.HTTPFailure
        with self.assertRaises(HTTPFailure):await c.rest(feed,'openInterest','s')
        self.assertGreater(c.budgets['binance-futures'].cooldown,time.monotonic()+19)
    async def test_healthy_requires_recent_and_liquidation_ack(self):
        # 資料過期不得健康；安靜清算必須先訂閱確認。
        c=Collector(configuration(),self.root,self.writer,None)
        f={'exchange':'binance','symbol':'BTCUSDT','channel':'trade','time_unit':'us'}
        s=c.stat(f);s['connected']=True;c.valid(f,NOW);s['last_valid_mono']=time.monotonic()-200
        self.assertEqual(c.health()['data_quality'],'degraded')
        f=dict(f,exchange='binance-futures',channel='forceOrder');s=c.stat(f);s['connected']=True
        self.assertFalse(c.health()['feeds']['binance-futures/BTCUSDT/forceOrder']['healthy'])
        s['subscribed']=True;self.assertTrue(c.health()['feeds']['binance-futures/BTCUSDT/forceOrder']['healthy'])

    async def test_raw_depth_is_not_merged_filtered_or_deduplicated(self):
        # 重複、缺口及交叉價位的原文全部保留；採集端只觀察序號並要求快照。
        from types import SimpleNamespace
        import aiohttp
        collector=Collector(configuration(),self.root,self.writer,None)
        feed={'exchange':'binance-futures','symbol':'BTCUSDT','channel':'depth@0ms','time_unit':'ms'}
        ctx=collector.context(feed);ctx['snapshot_needed']=False
        payload=dict(delta(),b=[['110','4']],a=[['101','3']])
        originals=[event('depth@0ms',payload)['raw']]*2+[event('depth@0ms',delta(105,104))['raw']]
        for text in originals:
            collector.receive_message(SimpleNamespace(type=aiohttp.WSMsgType.TEXT,data=text),
                                      NOW,feed,'s',ctx,'fixture://depth')
        self.assertNotIn('engine',ctx);self.assertNotIn('book_synced',collector.stat(feed))
        self.assertEqual(collector.stat(feed)['received'],3)
        self.assertTrue(ctx['snapshot_needed']);self.assertEqual(collector.events['depth_sequence_gap'],1)
        self.writer.close()
        records=list(read_records(self.root))
        self.assertEqual([r['raw'] for r in records if r['kind']=='ws'],originals)
        self.assertFalse(any(r.get('event')=='book_synced' for r in records))

    async def test_snapshot_health_requires_recent_response(self):
        # 尚未回應或已過期的快照必須列為採集異常，不能因深度連線存在就健康。
        collector=Collector(configuration(),self.root,self.writer,None)
        feed={'exchange':'binance-futures','symbol':'BTCUSDT','channel':'depth@0ms','time_unit':'ms'}
        collector.context(feed)
        snapshot_feed=dict(feed,channel='depthSnapshot')
        name='binance-futures/BTCUSDT/depthSnapshot'
        self.assertFalse(collector.health()['feeds'][name]['healthy'])
        collector.valid(snapshot_feed,NOW)
        self.assertTrue(collector.health()['feeds'][name]['healthy'])
        collector.stat(snapshot_feed)['last_valid_mono']=time.monotonic()-400
        self.assertFalse(collector.health()['feeds'][name]['healthy'])
    async def test_local_ws_raw_snapshots_and_oi(self):
        # 以本機 WS/REST 驗證定期原始快照與 OI 採集，不在採集端建立訂單簿。
        from aiohttp import web,ClientSession
        async def rest(request):
            # 本機 REST fixture：回傳固定 snapshot 或 OI，避免使用外部網路。
            if request.path.endswith('openInterest'):return web.json_response({'symbol':'BTCUSDT','openInterest':'3'})
            return web.json_response(snapshot())
        async def ws_handler(request):
            # 本機 WS fixture：連續送出可接合深度增量，供快照與重連驗證。
            ws=web.WebSocketResponse();await ws.prepare(request)
            sub=await ws.receive_json();await ws.send_json({'id':sub['id'],'result':None})
            stream=sub['params'][0]
            try:
                for i in range(101,121):
                    await ws.send_json({'stream':stream,'data':delta(i,i-1)})
                    await asyncio.sleep(.03)
            except (ConnectionResetError,RuntimeError):pass
            await ws.close();return ws
        app=web.Application();app.router.add_get('/stream',ws_handler);app.router.add_get('/depth',rest);app.router.add_get('/openInterest',rest)
        runner=web.AppRunner(app);await runner.setup();site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
        port=site._server.sockets[0].getsockname()[1]
        conf=configuration();conf['proxy']=None;conf['rest']['binance-futures']=f'http://127.0.0.1:{port}';conf['snapshot_interval']=.2
        feed={'exchange':'binance-futures','symbol':'BTCUSDT','channel':'depth@0ms','time_unit':'ms','urls':[f'http://127.0.0.1:{port}/stream']}
        try:
            async with ClientSession() as http:
                c=Collector(conf,self.root,self.writer,http);task=asyncio.create_task(c.stream(feed,0))
                await asyncio.sleep(.5)
                self.assertGreaterEqual(c.stat(dict(feed,channel='depthSnapshot'))['received'],2)
                self.assertNotIn('book_synced',c.stat(feed))
                oi=dict(feed,channel='openInterest');await c.rest(oi,'openInterest','oi')
                task.cancel();await asyncio.gather(task,return_exceptions=True)
                self.assertEqual(c.events['book_synced'],0)
                self.assertEqual(c.health()['order_book_validation'],'offline_only')
        finally:await runner.cleanup()

class FullPipelineTests(unittest.IsolatedAsyncioTestCase):
    """FullPipelineTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    async def rest(self, request):
        # 本機 REST fixture：回傳固定 snapshot 或 OI，避免使用外部網路。
        from aiohttp import web
        if request.path.endswith('openInterest'):
            return web.json_response({'symbol':request.query['symbol'],'openInterest':'3'})
        return web.json_response(snapshot())

    async def websocket(self, request):
        # 本機 WS fixture：按來源協議回應訂閱與固定訊息。
        from aiohttp import web
        ws=web.WebSocketResponse();await ws.prepare(request)
        sub=await ws.receive_json();stream=sub['params'][0];sym,ch=stream.split('@',1);sym=sym.upper()
        micro=request.query.get('timeUnit')=='microsecond'
        await ws.send_json({'id':sub['id'],'result':None});await asyncio.sleep(.08)
        try:
            for i in range(101,151):
                t=NOW+i*1000 if micro else NOW//1000+i
                if ch.startswith('depth'):d=dict(delta(i,i-1),s=sym,E=t)
                elif ch=='trade':d={'s':sym,'t':i,'T':t,'p':'100','q':'1','m':False}
                elif ch=='bookTicker':
                    d={'s':sym,'u':i,'a':'101','A':'2','b':'100','B':'3'}
                    if not micro:d['E']=t
                elif ch=='ticker':d={'s':sym,'E':t,'c':'100'}
                elif ch=='markPrice@1s':d={'s':sym,'E':t,'T':NOW//1000+5000,'p':'100','i':'101','r':'.001'}
                else:d={'o':{'s':sym,'T':t,'X':'FILLED','S':'SELL','p':'99','z':'1'}}
                await ws.send_json({'stream':stream,'data':d});await asyncio.sleep(.05)
        except (ConnectionResetError,RuntimeError):pass
        await ws.close();return ws

    async def test_eight_pairs_collect_process_compare(self):
        # 以完整本機八商品市場管線驗證 48 原始來源、32 正規化組合與重播一致性。
        from aiohttp import web
        from argparse import Namespace
        run = collect_entry.run
        app=web.Application();app.router.add_get('/stream',self.websocket);app.router.add_get('/depth',self.rest);app.router.add_get('/openInterest',self.rest)
        runner=web.AppRunner(app);await runner.setup();site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
        port=site._server.sockets[0].getsockname()[1];base=f'http://127.0.0.1:{port}'
        config=configuration();config.update(proxy=None,min_free_gib=.001,heartbeat_seconds=.2)
        config['spot_ws']=[base+'/stream'];config['futures_ws']={k:[base+'/stream'] for k in ('public','market','trade')}
        config['rest']={m:base for m in config['markets']}
        try:
            with tempfile.TemporaryDirectory() as d:
                root=Path(d);self.assertEqual(await run(Namespace(output=str(root/'data'),duration=2.2),config),0)
                # 現場與離線使用相同序列化，48 個原始來源必須保持分開。
                raw_run=next((root/'data').glob('*/manifest.json')).parent
                manifest=json.loads((raw_run/'manifest.json').read_text())
                self.assertEqual(sum(n>0 for n in manifest['channel_csv_data_counts'].values()),48)
                export_channels(root/'data',root/'channels_a');export_channels(root/'data',root/'channels_b')
                for path in (raw_run/'channels').rglob('*.csv'):
                    rel=path.relative_to(raw_run)
                    self.assertEqual(path.read_bytes(),(root/'channels_a'/raw_run.name/rel).read_bytes())
                    self.assertEqual(path.read_bytes(),(root/'channels_b'/raw_run.name/rel).read_bytes())
                a,b=root/'a',root/'b';r=process(root/'data',a);process(root/'data',b)
                nonempty={tuple(p.split('/')[:2])+(p.split('_')[-1],) for p,n in r['rows'].items() if n}
                self.assertEqual(len(nonempty),32)
                for path in a.rglob('*.csv'):self.assertEqual(path.read_bytes(),(b/path.relative_to(a)).read_bytes())
                comp=compare(a,b,root/'comparison',NOW-1,NOW+1000000)
                trade_rows=[x for x in comp['results'] if x['type']=='trades']
                self.assertEqual(len(trade_rows),8)
                self.assertTrue(all(x['shared_trade_ids']>0 and x['only_tardis_ids']==0 for x in trade_rows))
        finally:await runner.cleanup()

class SupervisorTests(unittest.TestCase):
    """SupervisorTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def check_restart(self,hang):
        # 用真實測試子程序模擬崩潰／卡住，確認監督器會啟動正確的新入口。
        import subprocess
        supervise = collect_entry
        real_popen=subprocess.Popen
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); counter=root/'counter'
            stub=root/'stub.py'
            stub.write_text("import pathlib,sys,time,signal\np=pathlib.Path(sys.argv[1]);n=int(p.read_text())+1 if p.exists() else 1;p.write_text(str(n))\nif n==1:\n signal.signal(signal.SIGTERM,lambda *_:sys.exit(0))\n "+("time.sleep(20)" if hang else "sys.exit(7)")+"\n")
            def launch(cmd,**kwargs):
                # 替換真正採集程序為可控制的子程序，並驗證啟動的是 01_collect.py。
                self.assertEqual(Path(cmd[2]).name,'01_collect.py')
                self.assertEqual(cmd[3],'run')
                return real_popen([sys.executable,str(stub),str(counter)],**kwargs)
            argv=['collect','supervise','--output',str(root),'--heartbeat-timeout','0.6','--restart-limit','1']
            with patch.object(collect_entry.subprocess,'Popen',side_effect=launch),patch('sys.argv',argv),patch.object(collect_entry.signal,'signal'):
                self.assertEqual(supervise.main(),0)
            self.assertEqual(counter.read_text(),'2')
    def test_crashed_child_restarts(self):
        # 子程序異常退出後，監督器必須建立新採集程序。
        self.check_restart(False)
    def test_hung_child_restarts_after_clean_sigterm(self):
        # 心跳卡住即使 SIGTERM 後返回零，也仍屬監督器觸發的重啟。
        self.check_restart(True)



# 原始 channel 必須保存每個訊息，重播結果須逐位元組一致。
DAY = 1792627200000000
CONFIG = {'markets': ['binance', 'binance-futures'],
          'symbols': ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'GRTUSDT']}


def received(channel, data, seq=1, **extra):
    # 建立原始 WS／REST 回應，測試時可以覆寫 seq、metadata 與時間。
    kind = 'rest' if channel in ('depthSnapshot', 'openInterest') else 'ws'
    raw = json.dumps(data if kind == 'rest' else {'stream': f'btcusdt@{channel}', 'data': data})
    return dict(seq=seq, kind=kind, exchange='binance-futures', symbol='BTCUSDT', channel=channel,
                session='s', local_timestamp=DAY, time_unit='ms', raw=raw, **extra)


def read_csv(path):
    # 讀回 CSV 測試結果，保留字串以檢查原始精度與尾端零。
    with path.open(newline='', encoding='utf8') as f:
        return list(csv.DictReader(f))


class OriginalChannelTests(unittest.TestCase):
    """OriginalChannelTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def test_no_merge_filter_dedup_timestamp_conversion_or_rounding(self):
        # 原始 channel CSV 不得合併、去重、改單位或丟棄非 FILLED／未同步資料。
        with tempfile.TemporaryDirectory() as d:
            writer = ChannelCSV(d, 'run')
            records = [
                received('ticker', {'s': 'BTCUSDT', 'E': 123, 'c': '100.0000'}),
                received('ticker', {'s': 'BTCUSDT', 'E': 123, 'c': '100.0000'}, seq=2),
                received('markPrice@1s', {'E': 124, 'p': '99.0100', 'i': '98', 'r': '.0001', 'T': 400}, seq=3),
                received('openInterest', {'symbol': 'BTCUSDT', 'openInterest': '5.000'}, seq=4, generated=True),
                received('trade', {'t': 7, 'p': '100.00', 'q': '0.0100', 'm': True, 'X': 'ADL'}, seq=5),
                received('trade', {'t': 7, 'p': '100.00', 'q': '0.0100', 'm': True, 'X': 'ADL'}, seq=6),
                received('forceOrder', {'o': {'X': 'PARTIALLY_FILLED', 'p': '97', 'z': '1', 'q': '8'}}, seq=7),
                received('depth@0ms', {'U': 500, 'u': 900, 'pu': 2, 'b': [['100.00', '0']], 'a': []}, seq=8),
                received('depthSnapshot', {'lastUpdateId': 1, 'bids': [['99', '2']], 'asks': [['101', '3']]}, seq=9),
            ]
            for record in records:
                writer.consume(record)
            writer.close()
            path = Path(d) / 'binance-futures/BTCUSDT' / utc(DAY)
            ticker = read_csv(path / 'ticker.csv')
            self.assertEqual(len(ticker), 2)
            self.assertEqual(ticker[0]['c'], '100.0000')
            self.assertEqual(ticker[0]['E'], '123')
            self.assertEqual(ticker[0]['capture_local_timestamp'], str(DAY))
            self.assertNotIn('open_interest', ticker[0])
            self.assertEqual(read_csv(path / 'markPrice@1s.csv')[0]['p'], '99.0100')
            self.assertEqual(read_csv(path / 'openInterest.csv')[0]['openInterest'], '5.000')
            trades = read_csv(path / 'trade.csv')
            self.assertEqual(len(trades), 2)
            self.assertEqual(trades[0]['m'], 'true')
            self.assertEqual(trades[0]['X'], 'ADL')
            self.assertEqual(trades[0]['p'], '100.00')
            self.assertEqual(json.loads(read_csv(path / 'forceOrder.csv')[0]['o'])['X'], 'PARTIALLY_FILLED')
            self.assertEqual(json.loads(read_csv(path / 'depth@0ms.csv')[0]['b']), [['100.00', '0']])
            self.assertFalse(list(Path(d).rglob('derivative_ticker.csv')))
            for record in records:
                rows = read_csv(path / (record['channel'] + '.csv'))
                row = next(r for r in rows if int(r['capture_seq']) == record['seq'])
                self.assertEqual(row['raw_message'], record['raw'])

    def test_controls_errors_binary_unknown_fields_and_exact_numbers(self):
        # 保留控制／錯誤／binary／新欄位及高精度原文，不能因解析失敗丟資料。
        r = received('ticker', {}, endpoint='wss://example.invalid', future_metadata={'test': True})
        raw = ' {"stream":"btcusdt@ticker","data":{"c":1.234567890123456789000,"E":123,"newField":"x,y\\n測試"}}\n'
        r['raw'] = raw
        row = channel_row(r, 'run')
        self.assertEqual(row['c'], '1.234567890123456789000')
        self.assertEqual(row['raw_message'], raw)
        self.assertEqual(json.loads(row['capture_extra'])['future_metadata'], {'test': True})
        binary = dict(r, encoding='base64', raw=base64.b64encode(raw.encode()).decode())
        self.assertEqual(channel_row(binary, 'run')['c'], row['c'])
        self.assertEqual(base64.b64decode(channel_row(binary, 'run')['raw_message']).decode(), raw)
        for raw, expected in [('not json', 'unparseable'), ('{"result":null,"id":1}', 'control'), ('[]', 'unexpected')]:
            row = channel_row(dict(r, raw=raw), 'run')
            self.assertEqual(row['capture_type'], expected)
            self.assertEqual(row['raw_message'], raw)
        self.assertEqual(channel_row(dict(r, raw='/w==', encoding='base64'), 'run')['capture_type'], 'unparseable')
        row = channel_row(dict(r, kind='rest_error', status=429, raw='<html>busy</html>'), 'run')
        self.assertEqual(row['capture_type'], 'rest_error')
        self.assertEqual(row['capture_status'], '429')

    def test_receive_date_rotation_48_headers_and_bounded_handles(self):
        # 依接收日期切檔並建立 48 個 header，限制同時開啟的檔案數。
        with tempfile.TemporaryDirectory() as d:
            writer = ChannelCSV(d, 'r', CONFIG, DAY, max_open=1)
            r = received('ticker', {'E': 1, 'c': '100'})
            for t in (DAY, DAY + 86400000000, DAY - 1, DAY):
                writer.consume(dict(r, local_timestamp=t))
                self.assertLessEqual(len(writer.open), 1)
            writer.close()
            self.assertEqual(len(list(Path(d).rglob('*.csv'))), 3 * 48)
            self.assertEqual(len(read_csv(Path(d) / f'binance-futures/BTCUSDT/{utc(DAY)}/ticker.csv')), 2)
            self.assertEqual(read_csv(Path(d) / f'binance-futures/GRTUSDT/{utc(DAY)}/forceOrder.csv'), [])

    def test_writer_and_two_replays_identical_across_restarts(self):
        # 不同 run 的原始資料分開重播，現場與兩份輸出必須逐位元組一致。
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for number in (1, 2):
                run = root / 'input' / f'run{number}'
                writer = Writer(run, min_free_gib=.001, channel_config=CONFIG, run_id=run.name, started_us=DAY)
                record = received('ticker', {'E': 123, 'c': '1.000'})
                record.pop('seq'); kind = record.pop('kind')
                writer.emit(kind, **record); writer.emit(kind, **record)
                writer.emit('event', event='disconnected', local_timestamp=DAY)
                writer.close()
                atomic_json(run / 'manifest.json', {'schema_version': 2, 'channel_csv_schema_version': 1,
                            'run_id': run.name, 'started_us': DAY, 'status': 'stopped', 'raw_records': 3, 'config': CONFIG})
                self.assertEqual(writer.channel_rows, 2)
            a, b = root / 'a', root / 'b'
            report = export_channels(root / 'input', a)
            export_channels(root / 'input', b)
            self.assertEqual(len(report['runs']), 2)
            for path in (root / 'input').rglob('*.csv'):
                rel = path.relative_to(root / 'input')
                self.assertEqual(path.read_bytes(), (a / rel).read_bytes())
                self.assertEqual(path.read_bytes(), (b / rel).read_bytes())
            with self.assertRaises(FileExistsError):
                export_channels(root / 'input', a)

    def test_channel_disk_failure_stops_writer_raw_remains_recoverable(self):
        # CSV 寫入失敗應停止 writer，但先落盤的 raw 仍可顯式恢復。
        with tempfile.TemporaryDirectory() as d:
            with patch.object(ChannelCSV, 'consume', side_effect=OSError('channel disk failed')):
                writer = Writer(d, min_free_gib=.001, channel_config=CONFIG, started_us=DAY)
                record = received('ticker', {'c': '1'}); record.pop('seq'); kind = record.pop('kind')
                writer.emit(kind, **record)
                with self.assertRaisesRegex(StorageError, 'channel disk failed'):
                    writer.close()
            self.assertEqual(len(list(read_records(d, recover=True))), 1)
            self.assertTrue(list((Path(d) / 'raw').glob('*.part')))

    def test_active_rejected_unclean_recovered_and_counts_checked(self):
        # 拒絕讀取活動 run，未正常關閉需 recover，筆數不符必須回報。
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); run = root / 'input/run'
            writer = Writer(run, min_free_gib=.001)
            record = received('ticker', {'c': '1'}); record.pop('seq'); kind = record.pop('kind')
            writer.emit(kind, **record); writer.close()
            manifest = {'schema_version': 2, 'started_us': DAY, 'status': 'running', 'config': CONFIG}
            atomic_json(run / 'manifest.json', manifest)
            with (run.parent / '.collector.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(ValueError, 'stop the collector'):
                    export_channels(run, root / 'active', recover=True)
            with self.assertRaisesRegex(ValueError, 'unclean'):
                export_channels(run, root / 'unclean')
            report = export_channels(run, root / 'recovered', recover=True)
            self.assertEqual(report['status'], 'completed_with_warnings')
            atomic_json(run / 'manifest.json', dict(manifest, status='stopped', raw_records=2))
            with self.assertRaisesRegex(ValueError, 'count mismatch'):
                export_channels(run, root / 'mismatch')




class ModularTests(unittest.TestCase):
    """ModularTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def test_market_adapters_and_protocol_boundaries(self):
        # 每個市場使用自己的訂閱與錯誤格式，不混用不相容商品代碼。
        from common.protocol import ADAPTERS
        adapter = transport.adapter
        for market in ADAPTERS:
            config=configuration(markets=[market]); actual=feeds(config)
            self.assertTrue(actual)
            for feed in actual:
                self.assertEqual(feed['exchange'],market)
                self.assertIn(feed['channel'],adapter(market).channels(market))
            error={'event':'error'} if market.startswith('okex') else ({'type':'error'} if market=='coinbase' else {'error':'bad'})
            self.assertEqual(adapter(market).classify(actual[0],error)[0],'error')
        with self.assertRaisesRegex(ValueError,'separately'):
            configuration(markets=['bybit','coinbase'])
        OKX = importlib.import_module("01_collector.collectors.okx").Collector
        Upbit = importlib.import_module("01_collector.collectors.upbit").Collector
        self.assertEqual(OKX.application_heartbeat,'ping');self.assertFalse(Upbit.requires_ack)
        self.assertEqual(configuration(markets=['coinbase'])['symbols'][0],'BTC-USD')
        self.assertEqual(configuration(markets=['okex-swap'])['symbols'][0],'BTC-USDT-SWAP')
    def test_non_binance_normalization_rejected(self):
        # 未實作的市場正規化必須拒絕，不產生空的假結果。
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);run=root/'input';run.mkdir()
            atomic_json(run/'manifest.json',{'schema_version':2,'started_us':NOW,'status':'stopped','raw_records':0,
                        'config':{'markets':['bybit'],'symbols':['BTCUSDT']}})
            with self.assertRaisesRegex(ValueError,'Binance only'):process(run,root/'out')
            self.assertFalse((root/'out').exists())
    def test_legacy_raw_read_and_mapping_without_duplicate_implementation(self):
        # 舊 JSONL 按 seq 重播到新 mapper，不保留第二套採集／轉換程式。
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);run=root/'old';(run/'raw').mkdir(parents=True)
            rows=[dict(event('depth@0ms',delta()),kind='event',event='connected'),
                  dict(event('depth@0ms',delta()),kind='message'),
                  dict(event('depthSnapshot',snapshot()),kind='data'),
                  dict(event('trade',{'s':'BTCUSDT','t':7,'p':'100.00','q':'1','m':False,'T':NOW//1000}),kind='message')]
            for seq,row in enumerate(rows,1):row['seq']=seq
            # 系統時鐘倒退造成分檔交錯，重播仍須依全域 seq 排序。
            (run/'raw/a.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows[::2]))
            (run/'raw/b.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in rows[1::2]))
            atomic_json(run/'manifest.json',{'raw_schema_version':1,'exchange':'binance-futures','symbol':'BTCUSDT',
                        'status':'complete','started_us':NOW,'ended_us':NOW+1,'raw_records':4})
            self.assertEqual([r['seq'] for r in read_records(run)],[1,2,3,4])
            report=process(run,root/'normalized')
            self.assertEqual(sum(n for p,n in report['rows'].items() if '/quotes/' in p),1)
            self.assertEqual(sum(n for p,n in report['rows'].items() if '/trades/' in p),1)
            raw_report=export_channels(run,root/'replay')
            self.assertEqual(raw_report['runs'][0]['csv_rows'],3)

class OtherMarketIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """OtherMarketIntegrationTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    cases={
        'bybit': ('publicTrade',{'topic':'publicTrade.BTCUSDT','data':[{'s':'BTCUSDT','p':'1.00'}]}),
        'bybit-spot': ('publicTrade',{'topic':'publicTrade.BTCUSDT','data':[{'s':'BTCUSDT','p':'1.00'}]}),
        'okex': ('trades',{'arg':{'channel':'trades','instId':'BTC-USDT'},'data':[{'px':'1.00'}]}),
        'okex-swap': ('trades',{'arg':{'channel':'trades','instId':'BTC-USDT-SWAP'},'data':[{'px':'1.00'}]}),
        'coinbase': ('matches',{'type':'last_match','product_id':'BTC-USD','price':'1.00'}),
        'upbit': ('trade',{'type':'trade','code':'KRW-BTC','trade_price':1})}

    async def websocket(self, request):
        # 本機 WS fixture：按來源協議回應訂閱與固定訊息。
        from aiohttp import web
        market=request.match_info['market']
        ws=web.WebSocketResponse();await ws.prepare(request);sub=await ws.receive_json()
        config=configuration(markets=[market]);channel,data=self.cases[market];symbol=config['symbols'][0]
        if market.startswith('bybit'):
            self.assertEqual(sub,{'op':'subscribe','args':[channel+'.'+symbol]})
            await ws.send_json({'op':'subscribe','success':True})
        elif market.startswith('okex'):
            self.assertEqual(sub,{'op':'subscribe','args':[data['arg']]})
            await ws.send_json({'event':'subscribe','arg':data['arg']})
        elif market=='coinbase':
            self.assertEqual(sub['product_ids'],[symbol])
            await ws.send_json({'type':'subscriptions','channels':[{'name':channel,'product_ids':[symbol]}]})
        else:
            self.assertEqual(sub[1],{'type':'trade','codes':[symbol]})
        payload=json.dumps(data)
        if market=='upbit':await ws.send_bytes(payload.encode())
        else:await ws.send_str(payload)
        await asyncio.sleep(.03);await ws.close();return ws

    async def check_market(self, market, channel, data, port):
        # 用真實本機連線驗證重連後原文仍完整保存，且每次連線有獨立 session。
        from aiohttp import ClientSession
        adapter = transport.adapter
        with tempfile.TemporaryDirectory() as d:
            config=configuration(markets=[market]);config.update(proxy=None,min_free_gib=.001)
            config['symbols']=config['symbols'][:1]
            writer=Writer(d,min_free_gib=.001,channel_config=config)
            feed=next(f for f in feeds(config) if f['channel']==channel);feed['urls']=[f'http://127.0.0.1:{port}/{market}']
            try:
                async with ClientSession() as http:
                    collector=adapter(market)(config,Path(d),writer,http)
                    with patch.object(transport.random, 'uniform',return_value=.01):
                        task=asyncio.create_task(collector.stream(feed))
                        try:
                            await asyncio.wait_for(wait_until(lambda: collector.stat(feed)['received'] >= 2), 4)
                            self.assertTrue(collector.stat(feed)['subscribed'])
                        finally:
                            task.cancel();await asyncio.gather(task,return_exceptions=True)
            finally:writer.close()
            raw=list(read_records(d));ws=[r for r in raw if r['kind']=='ws']
            self.assertGreaterEqual(len({r['session'] for r in ws}),2)
            paths=list((Path(d)/'channels').rglob(channel+'.csv'))
            rows=[r for p in paths for r in read_csv(p) if r['capture_type']=='data']
            self.assertGreaterEqual(len(rows),2)
            self.assertEqual(json.loads(rows[0]['payload']),data)

    async def test_independent_protocols_share_raw_writer_and_reconnect(self):
        # 每個市場依自己的訂閱協議連線，依序驗證共用傳輸與寫入流程。
        from aiohttp import web
        app = web.Application()
        app.router.add_get('/{market}', self.websocket)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            for market, (channel, data) in self.cases.items():
                await self.check_market(market, channel, data, port)
        finally:
            await runner.cleanup()

class LoggingTests(unittest.TestCase):
    """LoggingTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    def test_timestamp_format_rotation_and_console_not_duplicated(self):
        # 驗證每行時間前綴、traceback、日誌大小輪替，以及重複初始化不增生終端 handler。
        from common import LOG, configure_logging, close_log
        import io
        old_handlers, old_level, old_propagate = LOG.handlers[:], LOG.level, LOG.propagate
        LOG.handlers = []
        try:
            with tempfile.TemporaryDirectory() as d, patch('sys.stderr',new_callable=io.StringIO):
                path=Path(d)/'collector.log'
                handler=configure_logging(path,max_bytes=500,backups=2)
                configure_logging()
                self.assertEqual(sum(getattr(h,'_market_console',False) for h in LOG.handlers),1)
                for n in range(30):LOG.info('event=test n=%s payload=%s',n,'x'*100)
                try:raise RuntimeError('test failure')
                except RuntimeError:LOG.exception('event=example_error')
                close_log(handler)
                paths=list(Path(d).glob('collector.log*'))
                self.assertEqual(len(paths),3)
                lines=[line for p in paths for line in p.read_text().splitlines()]
                self.assertTrue(any('RuntimeError: test failure' in line for line in lines))
                for line in lines:
                    self.assertRegex(line,r'^\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{2}:\d{2}\] ')
                self.assertNotIn(handler,LOG.handlers)
        finally:
            for handler in LOG.handlers:handler.close()
            LOG.handlers=old_handlers;LOG.setLevel(old_level);LOG.propagate=old_propagate


class SourceStyleTests(unittest.TestCase):
    """維護可讀性規則，避免後續修改再次出現過長或缺少說明的函式。"""
    def test_function_length_and_opening_comments(self):
        # 掃描自行維護的程式與測試，包含裝飾器、註解及空白行的總長度。
        import ast
        import io
        import tokenize
        root = Path(__file__).resolve().parents[1]
        paths = [path for path in root.rglob('*.py') if 'output' not in path.relative_to(root).parts]
        for path in paths:
            source = path.read_text()
            lines = source.splitlines()
            for node in ast.walk(ast.parse(source)):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                label = f'{path.relative_to(root)}:{node.lineno} {node.name}'
                start = min([node.lineno] + [item.lineno for item in node.decorator_list])
                self.assertLessEqual(node.end_lineno - start + 1, 50, label)
                before = [line.strip() for line in lines[node.lineno:node.body[0].lineno - 1] if line.strip()]
                self.assertTrue(before and before[0].startswith('#'), label)
            # 中文字檢查阻止純英文註解；繁簡與內容是否正確仍需人工閱讀。
            for token in tokenize.generate_tokens(io.StringIO(source).readline):
                if token.type == tokenize.COMMENT and not token.string.startswith('#!'):
                    self.assertRegex(token.string, r'[\u4e00-\u9fff]', f'{path}:{token.start[0]}')

class HeartbeatLogTests(unittest.IsolatedAsyncioTestCase):
    """HeartbeatLogTests 測試組：集中驗證對應模組的行為與失敗邊界。"""
    async def test_periodic_healthy_log_and_degraded_transition(self):
        # 確認正常日誌定期出現，來源斷線後仍有存活摘要且明確標為 degraded。
        with tempfile.TemporaryDirectory() as d:
            writer=Writer(d,min_free_gib=.001)
            config=configuration();config['heartbeat_seconds']=.02
            collector=Collector(config,Path(d),writer,None)
            feed={'exchange':'binance','symbol':'BTCUSDT','channel':'trade','time_unit':'us'}
            state=collector.stat(feed);state.update(connected=True,subscribed=True)
            collector.valid(feed,NOW)
            try:
                with self.assertLogs('market-data',level='INFO') as logs:
                    task=asyncio.create_task(collector.heartbeat())
                    try:
                        await asyncio.wait_for(wait_until(lambda: sum('event=heartbeat ' in r.getMessage() for r in logs.records) >= 2), 2)
                        state['connected']=False
                        await asyncio.wait_for(wait_until(lambda: any('event=heartbeat ' in r.getMessage() and 'quality=degraded' in r.getMessage() for r in logs.records)), 2)
                    finally:task.cancel();await asyncio.gather(task,return_exceptions=True)
                pulses=[r for r in logs.records if 'event=heartbeat ' in r.getMessage()]
                self.assertGreaterEqual(len(pulses),3)
                self.assertTrue(all('process=running' in r.getMessage() for r in pulses))
                self.assertIn('quality=healthy',pulses[0].getMessage())
                self.assertEqual(pulses[0].levelname,'INFO')
                self.assertEqual(pulses[-1].levelname,'WARNING')
                self.assertIn('received=1',pulses[-1].getMessage())
                self.assertEqual(json.loads((Path(d)/'status.json').read_text())['data_quality'],'degraded')
            finally:writer.close()


class AiohttpCompatibilityTests(unittest.TestCase):
    """驗證新版設定及舊版數值參數都維持關閉逾時，避免在連線前崩潰。"""
    def test_modern_websocket_timeout(self):
        # 模擬新版公開 API；連線關閉限時兩秒，接收限時不由此設定。
        import aiohttp
        websocket_timeout = transport.websocket_timeout
        with patch.object(aiohttp, 'ClientWSTimeout', create=True) as factory:
            self.assertIs(websocket_timeout(), factory.return_value)
            factory.assert_called_once_with(ws_close=2)

    def test_missing_websocket_timeout_api(self):
        # 刻意移除公開屬性，重現使用者環境，確認不再拋出 AttributeError。
        import aiohttp
        websocket_timeout = transport.websocket_timeout
        with patch.dict(aiohttp.__dict__):
            aiohttp.__dict__.pop('ClientWSTimeout', None)
            self.assertEqual(websocket_timeout(), 2.0)
            self.assertEqual(websocket_timeout(3), 3.0)
