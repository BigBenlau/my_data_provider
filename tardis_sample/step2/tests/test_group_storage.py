"""驗證新線交接、滾動保留、歷史查重與跨盤重播。"""
from contextlib import ExitStack
import asyncio
import gzip
import importlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace
import test_redundancy as f

cold = importlib.import_module('01_collector.cold')
retention = importlib.import_module('01_collector.retention')
rotation = importlib.import_module('01_collector.ws_rotation')
segments = importlib.import_module('common.segments')
norm = importlib.import_module('02_normalize_tardis.normalization')
transport = importlib.import_module('01_collector.transport')


class StorageTests(unittest.TestCase):
    setUp = f.MergeTests.setUp
    tearDown = f.MergeTests.tearDown
    manifest = f.MergeTests.manifest

    def value(self, sql):
        # 每個斷言直接讀取已提交的 SQLite 指標。
        return self.store.db.execute(sql).fetchone()[0]

    def test_native_publication_and_normalization(self):
        # 同價成交的外層時間變更須保留於 01，02 再依成交欄位決定輸出。
        base = dict(topic='publicTrade.BTCUSDT',ts=1000,data=[dict(i='x',p='1.00',v='2',T=777,S='Buy',s='BTCUSDT')])
        item = dict(f.event(),exchange='bybit',channel='publicTrade',raw=json.dumps(base))
        second = dict(item,raw=json.dumps(dict(base,ts=1001)))
        reordered = dict(item,raw=json.dumps(base,sort_keys=True,indent=2))
        f.send(self.store,'a',[item])
        f.send(self.store,'b',[reordered,second])
        self.assertEqual(self.value('SELECT count(*) FROM observations'),3)
        self.assertEqual(self.value('SELECT count(*) FROM conflicts'),1)
        self.store.seal()
        self.manifest(item['local_timestamp'])
        rows = list(f.read_records(self.root))
        self.assertEqual([json.loads(r['raw'])['ts'] for r in rows],[1000,1001])
        self.assertEqual(rows[1]['conflict_category'],'publication_timestamp_variant')
        mapper = importlib.import_module('02_normalize_tardis.mappers.bybit').Mapper(sqlite3.connect(':memory:'))
        published = [r for row in rows for r in mapper.consume(row)]
        self.assertEqual(len(published),1)

    def test_exact_types_precision_and_sibling_overlap(self):
        # 任意巢狀欄位保留數字精度、型別及陣列順序，忽略同批其他成交。
        prefix = '{"topic":"publicTrade.BTCUSDT","ts":100,"data":['
        trade = '{"s":"BTCUSDT","i":"x","v":"1","p":"2","S":"Buy","T":50}'
        base = dict(f.event(),exchange='bybit',channel='publicTrade')
        def digest(items,extra=''):
            # 保留測試中的十進位字面值，避免測試本身經過 float。
            raw = prefix+','.join(items)+']'+extra+'}'
            return f.atoms(dict(base,raw=raw))[0]['digest']
        self.assertEqual(digest([trade]),digest([trade,trade.replace('"x"','"y"')]))
        values = ['',',"v":null',',"v":"1.1"',',"v":1.1',',"v":1.10000000000000000000001',',"v":[1,2]',',"v":[2,1]']
        self.assertEqual(len({digest([trade],v) for v in values}),len(values))
        self.assertEqual(digest([trade],',"v":1.10'),digest([trade],',"v":1.1'))

    def test_rollup_once_and_protected_evidence(self):
        # 模擬跨越 24 小時，已共同觀察、弱來源和事故明細分開計數。
        old = int((time.time()-86401)*1e6)
        trade = f.event(local=old)
        f.send(self.store,'a',[trade,f.event('ticker',local=old),f.event(number=9,local=old)])
        f.send(self.store,'b',[trade])
        with self.store.db:
            self.store.db.execute('INSERT INTO protected_seq VALUES(3)')
        for _ in range(2):
            self.store.archive.move(86400)
        self.assertEqual(self.value('SELECT sum(common) FROM hourly'),2)
        self.assertEqual(self.value('SELECT sum(unverified) FROM hourly'),1)
        self.assertEqual(self.value('SELECT sum(received) FROM hourly'),3)
        self.assertEqual(self.value('SELECT count(*) FROM observations'),1)
        f.send(self.store,'c',[dict(trade,local_timestamp=time.time_ns()//1000)])
        self.assertEqual(self.value('SELECT strong FROM observations WHERE seq=1'),-1)

    def test_hot_rollup_respects_batch_and_incident_ranges(self):
        # 多次接收同一事件時分批彙總，另一個來源即使有事故也不阻擋此來源。
        ts = int((time.time()-86410)*1e6)
        rec = f.event(local=ts)
        f.send(self.store,'a',[rec]*5 + [f.event('ticker',local=ts)])
        f.send(self.store,'b',[rec])
        db = self.store.db
        with db:
            db.execute('INSERT INTO handovers VALUES(?,?,NULL,?)',
                       (self.store.feed_key('binance-futures/BTCUSDT/ticker'),ts/1e6-1,'trial'))
        for remaining in (5,3,1):
            with db:
                self.assertEqual(self.store.evidence.summarize(time.time()-86400,2),2)
            self.assertEqual(self.value('SELECT count(*) FROM observations'),remaining)
        self.assertEqual(self.value('SELECT sum(common) FROM hourly'),6)
        self.assertEqual(self.value('SELECT count(*) FROM rolling'),0)

    def test_gap_rechecked_only_on_relevant_edge(self):
        # 跨越五秒期限只回報，無關來源及無交集的新邊均不重掃舊缺口。
        f.send(self.store,'a',[f.event('depth@0ms',1),f.event('depth@0ms',3)])
        monitor=f.Monitor(self.store,self.config)
        with patch.object(monitor,'chain',wraps=monitor.chain) as check:
            monitor.resolve_gaps()
            count=check.call_count
            self.assertGreater(count,0)
            f.send(self.store,'b',[f.event()])
            monitor.resolve_gaps()
            self.assertEqual(check.call_count,count)
            f.send(self.store,'b',[f.event('depth@0ms',2)],2)
            monitor.resolve_gaps()
            self.assertGreater(check.call_count,count)
            self.assertEqual(self.value('SELECT count(*) FROM gaps WHERE resolved<>1'),0)

    def test_five_adapters_repeatable_raw_and_csv(self):
        # 五家同一份 A/B 輸入重播兩次，比對正式 JSONL 與各原生 channel CSV。
        fixed = 1704067200000000
        families = [('binance','binance-futures','trade','BTCUSDT'),('bybit','bybit','publicTrade','BTCUSDT'),
                    ('coinbase','coinbase','matches','BTC-USD'),('okx','okex','trades','BTC-USDT'),
                    ('upbit','upbit','trade','KRW-BTC')]
        for family,market,channel,symbol in families:
            _,native=f.RuntimeTests.fixture(self,market,42)
            record=dict(f.event(local=fixed),exchange=market,channel=channel,symbol=symbol,raw=json.dumps(native))
            result=[]
            for run in ('first','second'):
                root=self.root/family/run
                config=f.config(family)
                store=f.MergeStore(root,config)
                try:
                    f.send(store,'a',[record])
                    f.send(store,'b',[dict(record,session='peer',raw=json.dumps(native,sort_keys=True,indent=2))])
                finally:
                    store.close()
                f.atomic_json(root/'manifest.json',dict(schema_version=3,status='stopped',raw_records=1,
                              config=config,started_us=fixed,run_id="fixed"))
                target=root/'export'
                f.Exporter(root,target).run()
                raw=b''.join(p.read_bytes() for p in sorted((root/'raw').rglob('*.jsonl')))
                result.append((raw,{p.relative_to(target):p.read_bytes() for p in target.rglob('*.csv.gz')}))
            self.assertEqual(result[0],result[1],family)

    def cold_setup(self, stack):
        # 一般主機以測試磁碟代替裝置檢查，容器驗收可直接使用不同的實際掛載。
        external = os.environ.get('TEST_COLD_PARENT')
        disk = Path(stack.enter_context(tempfile.TemporaryDirectory(dir=external)))
        (disk/'.collector-volume-id').write_text('fixture')
        stack.enter_context(patch.dict(os.environ,{'COLD_ROOT':str(disk),'COLD_VOLUME_ID':'fixture'}))
        if external is None:
            def fixture_volume(root,binding):
                # 身分不符時仍拒絕讀寫，模擬冷盤被拔出及換回。
                if (disk/'.collector-volume-id').read_text()!='fixture':
                    raise OSError('wrong test volume')
                return disk/binding['directory']
            stack.enter_context(patch.object(cold,'volume',fixture_volume))
            stack.enter_context(patch.object(segments,'volume',fixture_volume))
        self.store.cold.configure()
        self.assertIsNotNone(self.store.cold.binding)
        return disk

    def test_cold_unplug_sparse_ack_restart(self):
        # 冷盤不可用時同來源等待，不同來源繼續發布，重啟重送後只清前綴。
        with ExitStack() as stack:
            disk=self.cold_setup(stack)
            old = f.event(local=int((time.time()-86410)*1e6))
            f.send(self.store,'a',[old])
            self.store.archive.move(86400)
            self.store.cold.migrate(256)
            self.assertEqual(self.value('SELECT count(*) FROM archive.seen'),0)
            (disk/'.collector-volume-id').rename(disk/'offline')
            batch = [old,f.event(number=2),f.event('ticker')]
            self.assertEqual(f.send(self.store,'b',batch),0)
            self.assertEqual(self.value('SELECT written FROM output'),2)
            self.assertEqual(self.value('SELECT count(*) FROM waiting'),2)
            self.store.close()
            self.store=f.MergeStore(self.root,self.config)
            self.assertEqual(f.send(self.store,'b',batch),0)
            (disk/'offline').rename(disk/'.collector-volume-id')
            self.assertEqual(f.send(self.store,'b',batch),3)
            self.assertEqual(f.send(self.store,'b',batch),3)
            self.assertEqual(self.value('SELECT written FROM output'),3)
            self.assertEqual(self.value('SELECT count(*) FROM waiting'),0)
            self.assertEqual(self.value('SELECT count(*) FROM receipts'),0)

    def files(self, label):
        # 核對完整正式 JSONL、01 解壓後 CSV 及 02 所有 CSV 的實際位元組。
        export=self.root/('export-'+label)
        normalized=self.root/('normalized-'+label)
        f.Exporter(self.root,export).run()
        mapper = importlib.import_module('02_normalize_tardis.mappers.binance').Mapper
        norm.replay(mapper,self.root,normalized)
        return (list(f.read_records(self.root)), {p.relative_to(export):gzip.decompress(p.read_bytes()) for p in export.rglob('*.csv.gz')},
                {p.relative_to(normalized):p.read_bytes() for p in normalized.rglob('*.csv')})

    def test_cross_disk_replay_and_copy_interruption(self):
        # 同一固定輸入經全熱、混合、全冷三次重播，來源損壞要指出確切序號。
        with ExitStack() as stack:
            self.cold_setup(stack)
            current = time.time_ns()//1000
            for n in (1,2):
                f.send(self.store,'a',[f.event(number=n,local=current+n)],n)
                self.store.seal()
            self.manifest(current)
            retention.compress_ready(self.root)
            before=self.files('hot')
            binding=self.store.cold.binding
            with patch.object(cold,'validate_segment',side_effect=OSError('copy interrupted')):
                with self.assertRaises(OSError):
                    cold.archive_files(self.root,binding,-1)
            self.assertEqual(len(list((self.root/'raw').rglob('*.gz'))),2)
            cold.archive_files(self.root,binding,-1)
            self.assertEqual(self.files('mixed'),before)
            cold.archive_files(self.root,binding,-1)
            self.assertEqual(self.files('cold'),before)
            archived=sorted((cold.volume(self.root,binding)/'raw').rglob('*.gz'))
            archived[-1].write_bytes(b'truncated')
            with self.assertRaisesRegex(OSError,r'seq=2\.\.2'):
                list(f.read_records(self.root))

    def test_handshake_cooldown_shared_after_restart(self):
        # A/B 共用持久握手限制，新程序也要遵守同一次 429 的期限。
        path=self.root/'schedule.sqlite'
        a=f.Schedule(path,f.config('upbit'))
        self.assertTrue(a.connection('upbit'))
        self.assertFalse(a.connection('upbit'))
        a.pause_ws(20)
        a.close()
        b=f.Schedule(path,f.config('upbit'))
        try:
            with patch.object(b,'now',return_value=time.time()+10):
                self.assertFalse(b.connection('upbit'))
            with patch.object(b,'now',return_value=time.time()+21):
                self.assertTrue(b.connection('upbit'))
        finally:
            b.close()

    def test_same_disk_rejected(self):
        # 操作員忘記掛載第二顆磁碟時不得在系統碟建立封存目錄。
        with tempfile.TemporaryDirectory(dir=self.root) as path:
            (Path(path)/'.collector-volume-id').write_text('fixture')
            with patch.dict(os.environ,{'COLD_ROOT':path}):
                with self.assertRaisesRegex(OSError,'share device'):
                    cold.initialize(self.root,'fixture')


class RotationTests(unittest.TestCase):
    def setUp(self):
        # 用真實提交證據驅動排程，測試用時鐘跳過十分鐘排程及觀察期限。
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config=f.config()
        self.store=f.MergeStore(self.temp.name,self.config)
        self.addCleanup(self.store.close)
        self.mon=f.Monitor(self.store,self.config)
        self.sup=SimpleNamespace(monitor=self.mon,options=self.config['redundancy'],config=self.config,
                                 state=dict(active=['a','b'],last_rotation=0),repair={},save=lambda:None)
        self.rotation=rotation.Rotation(self.sup)
        self.now=time.time()
        self.gid,group=next((g,s) for g,s in self.mon.groups.items() if g.startswith('binance-futures/trade/'))
        self.name=transport.key(group['feeds'][0])
        for w in ('a','b'):
            state=dict(session=w,connected=True,subscribed=True,first_data_us=1,session_started_us=int(self.now*1e6))
            self.mon.status(w,dict(feeds={self.name:state}))

    def test_rotation_requires_three_sessions_then_alternates(self):
        # 同一 worker 的兩個 session 不可互相覆寫，收到共同原生事件才提升。
        self.rotation.tick(self.now,False)
        trial=self.rotation.s['trial']
        wid=trial['worker']
        self.mon.current[wid]['lines']={self.gid:dict(token=trial['token'],feeds={self.name:dict(session='c',connected=True,subscribed=True,first_data_us=1)})}
        for worker,session,start in [('a','a',1),('b','b',1),(wid,'c',4)]:
            f.send(self.store,worker,[dict(f.event(number=i,local=int((self.now+1)*1e6)),session=session) for i in range(3)],start)
        self.rotation.tick(self.now+11,False)
        self.assertEqual(self.rotation.command(wid)['action'],'promote')
        self.mon.current[wid]['lines'][self.gid]['completed']=trial['token']
        self.rotation.tick(self.now+12,False)
        self.assertEqual(self.rotation.s['history'][-1]['reason'],'verified_replacement')
        self.rotation.tick(self.now+15,False)
        self.assertNotIn('trial',self.rotation.s)
        self.rotation.tick(self.now+620,False)
        self.assertNotEqual(self.rotation.s['trial']['worker'],wid)

    def test_gap_on_live_group_bypasses_preventive_schedule(self):
        # 即使收訊仍向前，剛發生的序號缺口也須立即啟動相同群組的候選線。
        group = next(g for g in self.mon.groups.values() if g['id'].startswith('binance-futures/depth@0ms/'))
        name=transport.key(group['feeds'][0])
        for w,state in self.mon.current.items():
            state['feeds'][name]=dict(state['feeds'][self.name])
        f.send(self.store,'a',[dict(f.event('depth@0ms',i),session='a') for i in (1,3)])
        self.sup.repair,_=self.mon.repairs(['a','b'],self.mon.states())
        self.sup.state['last_rotation']=self.now
        self.rotation.tick(self.now+1,True)
        self.assertEqual(self.rotation.s['trial']['group'],group['id'])
        self.assertEqual(self.rotation.s['trial']['trigger'],'sequence_gap')

    def test_backlog_age_and_weak_source(self):
        # 積壓暫停例行換線；弱來源超齡仍開候選，逾時只撤回候選。
        self.rotation.tick(self.now,True)
        self.assertEqual(self.rotation.s['history'][-1]['reason'],'delivery_backlog')
        self.mon.blockers[self.name]='weak'
        for v in self.mon.current.values():
            v['feeds'][self.name]['session_started_us']=1
        self.rotation.tick(self.now+620,True)
        trial=self.rotation.s['trial']
        self.assertEqual(trial['trigger'],'age_limit')
        self.rotation.tick(trial['expires']+1,True)
        self.assertNotIn('trial',self.rotation.s)
        self.assertEqual(self.sup.state['active'],['a','b'])

    def test_emergency_and_pressure_hysteresis(self):
        # 異常通道可立即申請候選，積壓解除須連續三個低水位心跳。
        self.sup.state['last_rotation']=self.now
        self.sup.repair={'a':[dict(group=self.gid,session='a')]}
        self.rotation.tick(self.now,True)
        self.assertEqual(self.rotation.s['trial']['trigger'],'receive_stalled')
        self.mon.current['a']['oldest_pending_s']=11
        self.mon.pressure(['a'],self.mon.current)
        self.assertTrue(self.mon.backlogged)
        self.mon.current['a']['oldest_pending_s']=0
        for expected in (True,True,False):
            self.mon.current['a']['reported_at']=time.time()
            self.mon.pressure(['a'],self.mon.current)
            self.assertEqual(self.mon.backlogged,expected)


class SocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_old_worker_spool_pauses_only_prevention(self):
        # 舊事故正在補交，仍須把現役群組交給排程器處理緊急與超齡換線。
        supervisor=importlib.import_module('01_collector.supervisor').Supervisor
        callback=object()
        target=SimpleNamespace(state=dict(retiring=None,candidate=None),storage_holds=lambda:{'old'},
                               in_trial=lambda:False,monitor=SimpleNamespace(backlogged=False),output_fault=None,
                               ws_rotation=SimpleNamespace(tick=callback),locked=AsyncMock())
        await supervisor.rotation(target)
        args=target.locked.await_args.args
        self.assertIs(args[0],callback)
        self.assertIs(args[2],True)

    async def test_live_session_handover(self):
        # 本機 WS 伺服器同時發布同一事件，完整走過第三條線、驗證、關閉舊線。
        from aiohttp import web
        self.peers = set()
        self.maximum = 0
        async def endpoint(request):
            # 每條連線依當下時鐘發布相同原生事件，並記錄實際同時在線數。
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.receive_json()
            await ws.send_json({'result':None,'id':1})
            self.peers.add(ws)
            self.maximum=max(self.maximum,len(self.peers))
            try:
                while not ws.closed:
                    n=int(time.time()*10)
                    await ws.send_str(f.event(number=n)['raw'])
                    await asyncio.sleep(.1)
            except (ConnectionError,OSError):
                pass
            finally:
                self.peers.discard(ws)
            return ws
        app=web.Application()
        app.router.add_get('/binance-futures',endpoint)
        runner=web.AppRunner(app)
        await runner.setup()
        try:
            site=web.TCPSite(runner,'127.0.0.1',0)
            await site.start()
            await self.exchange(site._server.sockets[0].getsockname()[1])
        finally:
            await runner.cleanup()

    async def exchange(self,port):
        # 只加速十分鐘排程，原始收訊、IPC、三路證據及十秒觀察均走正式路徑。
        supervisor=importlib.import_module('01_collector.supervisor').Supervisor
        value,feed=f.RuntimeTests.settings(self,'binance-futures',port)
        value['redundancy'].update(candidate_timeout=35,verify_seconds=10)
        with tempfile.TemporaryDirectory() as root, patch.object(f.adapter('binance-futures'),'feeds',return_value=[feed]):
            runtime=f.LocalRuntime(root,value)
            sup=supervisor(root,value,runtime)
            task=asyncio.create_task(sup.serve())
            try:
                await asyncio.wait_for(self.observe_handover(sup),35)
            finally:
                sup.stop.set()
                await task
                sup.store.close()
                sup.schedule.close()

    async def observe_handover(self,sup):
        # 原線仍在收訊時才啟動交接，再檢查第三條線真正退出。
        while not all(any(v['received'] for v in s['feeds'].values()) for s in sup.monitor.current.values()) or len(sup.monitor.current)<2:
            await asyncio.sleep(.1)
        workers=list(sup.state['active'])
        sup.state['last_rotation']=0
        while not any(h['reason']=='verified_replacement' for h in sup.ws_rotation.s['history']):
            await asyncio.sleep(.1)
        self.assertEqual(workers,sup.state['active'])
        self.assertEqual(self.maximum,3)
        while len(self.peers)!=2:
            await asyncio.sleep(.1)
