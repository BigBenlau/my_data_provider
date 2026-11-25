"""實際運行問題的回歸：修復風暴、持久化積壓、歷史索引與封存重播。"""
import importlib
import asyncio
import fcntl
import gzip
import json
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import test_redundancy as fixtures
from test_redundancy import config, event, send
key = importlib.import_module('01_collector.transport').key
Spool = importlib.import_module("01_collector.delivery").Spool
monitor_module = importlib.import_module("01_collector.replica_monitor")
Monitor = monitor_module.Monitor
DEFAULTS = importlib.import_module("01_collector.redundancy").DEFAULTS
Schedule = importlib.import_module("01_collector.rest_schedule").Schedule
Exporter = importlib.import_module("01_collector.channel_export").Exporter
from common.raw_reader import read_records
export_channels = importlib.import_module("01_collector.raw").export_channels
worker_module = importlib.import_module("01_collector.worker")
Worker = worker_module.Worker


class RepairTests(unittest.TestCase):
    """整組收訊與新 session 寬限期避免冷門商品造成連鎖重連。"""
    setUp = fixtures.MergeTests.setUp
    tearDown = fixtures.MergeTests.tearDown

    def monitor(self):
        # 同條連線包含活躍 SOL 與靜默 GRT，模擬實際修復風暴的條件。
        self.config['market_settings'] = {'binance-futures': dict(symbols=['SOLUSDT', 'GRTUSDT'], channels=['trade'])}
        self.config['markets'] = ['binance-futures']
        return Monitor(self.store, self.config)

    def state(self, now, received, counter=10, session='s'):
        # 狀態包含真正回報時間及連線起點，不讓舊狀態冒充新收訊。
        return dict(reported_at=now, feeds={f'binance-futures/{symbol}/trade': dict(
            connected=True, subscribed=True, session=session, session_started_us=int((now-100)*1e6),
            last_data_us=int((received if symbol == 'SOLUSDT' else now-100)*1e6), received=counter)
            for symbol in ('SOLUSDT', 'GRTUSDT')})

    def test_quiet_symbol_does_not_restart_active_group(self):
        # GRT 沒有新成交時，同組 SOL 持續收訊就不應重連。
        monitor = self.monitor()
        for now in (1000, 1010, 1020):
            states = {'a': self.state(now, now), 'b': self.state(now, now-1)}
            with patch.object(monitor_module.time, 'time', return_value=now): commands, _ = monitor.repairs(['a', 'b'], states)
            self.assertEqual(commands, {'a': [], 'b': []})

    def test_stall_requires_peer_progress_and_is_session_bound(self):
        # 持續停滯加三筆以上跨路進度才下指令；恢復或換線後指令失效。
        monitor = self.monitor()
        for now, count in ((1000, 10), (1006, 15)):
            states = {'a': self.state(now, 900), 'b': self.state(now, now, count)}
            with patch.object(monitor_module.time, 'time', return_value=now): commands, _ = monitor.repairs(['a', 'b'], states)
        command = commands['a'][0]; group = next(iter(monitor.groups.values()))
        target = SimpleNamespace(repair={group['id']: command}, stats=states['a']['feeds'])
        with patch.object(worker_module.time, 'time', return_value=1006): self.assertFalse(Worker.repair_due(target, group, 'new'))
        target.repair[group['id']] = command
        for value in target.stats.values(): value['last_data_us'] = 1006000000
        with patch.object(worker_module.time, 'time', return_value=1006): self.assertFalse(Worker.repair_due(target, group, 's'))
        with patch.object(monitor_module.time, 'time', return_value=1007): self.assertEqual(monitor.repairs(['a', 'b'], states)[0]['a'], [])

    def test_stale_control_and_new_session_do_not_trigger_repair(self):
        # 過期狀態及新連線不參與落後判定，避免延遲回報觸發新一輪修復。
        monitor = self.monitor(); states = {'a': self.state(900, 800), 'b': self.state(1000, 1000)}
        with patch.object(monitor_module.time, 'time', return_value=1000): self.assertEqual(monitor.repairs(['a', 'b'], states)[0]['a'], [])
        state = self.state(1000, 0)
        for value in state['feeds'].values(): value['session_started_us'] = 999000000
        states['a'] = state
        with patch.object(monitor_module.time, 'time', return_value=1000): self.assertEqual(monitor.repairs(['a', 'b'], states)[0]['a'], [])

    def test_strict_preflight_identifies_unsupported_sources(self):
        # 明確列出不可能通過的來源；共同成交不能掩蓋 ticker 的未驗證狀態。
        self.config['market_settings'] = {'binance-futures': dict(symbols=['BTCUSDT'], channels=['trade', 'ticker'])}
        monitor = Monitor(self.store, self.config)
        self.assertEqual(monitor.blockers, {'binance-futures/BTCUSDT/ticker': 'no_reliable_native_identity'})
        self.assertFalse(monitor.verify('c', ['a', 'b'], time.time()-20)[0])

    def test_stopped_report_keeps_missing_required_sources(self):
        # 停止狀態不能掩蓋整段採集沒有取得必要來源的事實。
        monitor = self.monitor(); monitor.health([]); report = monitor.stopped()
        self.assertEqual(report['status'], 'stopped')
        self.assertEqual(report['data_quality'], 'incomplete')
        self.assertIn('binance-futures/SOLUSDT/trade', report['missing_required_sources'])

    def test_coinbase_heartbeat_is_not_counted_as_market_data(self):
        # 心跳只更新協議進度，不製造正式行情或共同事件證據。
        feed = dict(exchange='coinbase',symbol='BTC-USD',channel='matches')
        target = SimpleNamespace(stats={key(feed):dict(received=0)})
        Worker.application_control(target,dict(feeds=[feed]),dict(type='heartbeat',product_id='BTC-USD',last_trade_id=50,sequence=80))
        self.assertEqual(target.stats[key(feed)]['received'],0)
        self.assertEqual(target.stats[key(feed)]['heartbeat_trade_id'],50)

    def test_retired_worker_health_is_removed(self):
        # 長期換代不累積完整舊健康快照或舊連線標記，合併序號仍保留。
        monitor = self.monitor(); monitor.status('a', self.state(time.time(), time.time()))
        send(self.store, 'a', [event('depth@0ms',1)]); monitor.health(['a']); monitor.forget('a')
        self.assertNotIn('a',monitor.states())
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM markers WHERE worker='a'").fetchone()[0],0)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM markers WHERE worker='__merged__'").fetchone()[0],1)

    def test_duplicate_ipc_handshake_is_retryable(self):
        # 暫時重疊的舊 socket 不得結束 worker，真正身分不符仍拒絕。
        with self.assertRaises(ConnectionError): Worker.check_welcome(dict(error='duplicate connection',retry=True))
        with self.assertRaises(ValueError): Worker.check_welcome(dict(error='identity mismatch'))
        self.assertIsNone(Worker.check_welcome(dict(accepted=True)))


class PersistenceTests(unittest.TestCase):
    """歷史去重、分類及 CSV 復原不能因效能優化而失去語義。"""
    setUp = fixtures.MergeTests.setUp
    tearDown = fixtures.MergeTests.tearDown
    manifest = fixtures.MergeTests.manifest

    def age(self):
        # 只調整測試觀察時間，讓小 fixture 走到真實歷史封存路徑。
        with self.store.db:
            self.store.db.execute('UPDATE seen SET received=1')
            self.store.db.execute('UPDATE observations SET observed=1')

    def test_archive_late_copy_and_conflict(self):
        # 熱索引移除後仍識別舊副本，較晚的不同價量必須保存。
        original = event(); send(self.store, 'a', [original]); self.age()
        self.assertEqual(self.store.archive.move(60), 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM seen').fetchone()[0], 0)
        send(self.store, 'b', [original]); self.assertEqual(self.store.db.execute('SELECT written FROM output').fetchone()[0], 1)
        send(self.store, 'b', [dict(original, raw=original['raw'].replace('100.000001','100.000002'))], 2)
        self.assertEqual(self.store.db.execute('SELECT written FROM output').fetchone()[0], 2)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM conflicts').fetchone()[0], 1)

    def deny_delete(self, action, table, column, database, trigger):
        # 在歷史副本提交後注入刪除失敗，模擬跨資料庫邊界崩潰。
        return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_DELETE and database == 'main' else sqlite3.SQLITE_OK

    def test_archive_crash_between_copy_and_delete(self):
        # 複製與刪除之間失敗最多留兩份索引，不能遺失去重證據。
        original = event(); send(self.store, 'a', [original]); self.age()
        self.store.db.set_authorizer(self.deny_delete)
        with self.assertRaises(sqlite3.DatabaseError): self.store.archive.move(60)
        self.store.db.set_authorizer(None)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM archive.seen').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM seen').fetchone()[0], 1)
        self.assertEqual(self.store.archive.move(60), 1)
        send(self.store, 'b', [original]); self.assertEqual(self.store.db.execute('SELECT written FROM output').fetchone()[0], 1)

    def test_archived_edges_still_resolve_gaps(self):
        # 舊邊已封存時仍可證明完整序號鏈，不能只看熱索引誤報缺口。
        send(self.store, 'a', [event('depth@0ms', n) for n in (1, 3)])
        send(self.store, 'b', [event('depth@0ms', n) for n in (1, 2, 3)])
        self.age(); self.store.archive.move(60)
        self.assertEqual(Monitor(self.store, self.config).resolve_gaps(), [])
        self.assertTrue(all(r[0] == 1 for r in self.store.db.execute('SELECT resolved FROM gaps')))

    def test_seal_rename_crash_recovers_segment_catalog(self):
        # 封存已改名、資料庫尚未提交時重啟，CSV 必須仍看得到完整段。
        MergeStore = importlib.import_module("01_collector.delivery").MergeStore
        send(self.store, 'a', [event()])
        name = self.store.db.execute('SELECT path FROM output').fetchone()[0]
        path = self.root/name; path.rename(path.with_suffix(''))
        self.store.db.close(); self.store = MergeStore(self.root, self.config)
        self.assertEqual(self.store.db.execute('SELECT last_seq FROM segments').fetchone()[0], 1)
        self.assertIsNone(self.store.db.execute('SELECT path FROM output').fetchone()[0])

    def test_timestamp_variant_preserves_both_originals(self):
        # 同一更新的發布時間差只分類，正式原文與嚴格衝突證據仍保留。
        left = event('depth@100ms', market='binance'); native = json.loads(left['raw']); native['data']['E'] += 1
        right = dict(left, raw=json.dumps(native)); send(self.store, 'a', [left]); send(self.store, 'b', [right])
        self.store.seal(); self.manifest(left['local_timestamp']); records = list(read_records(self.root))
        self.assertEqual([r['raw'] for r in records], [left['raw'], right['raw']])
        self.assertEqual(records[1]['conflict_category'], 'publication_timestamp_variant')
        self.assertTrue(records[1]['content_conflict'])

    def test_csv_crash_and_compressed_replay(self):
        # 寫完 CSV 尚未提交游標時中止，下次截回並重播，壓縮後結果仍一致。
        compress_ready = importlib.import_module("01_collector.retention").compress_ready
        started = 1704067199000000
        target = self.root / 'export'
        send(self.store, 'a', [event(local=started)])
        self.store.seal(); self.manifest(started)
        self.assertEqual(Exporter(self.root, target).run()['rows'], 1)
        # 一個新日期與一筆舊日期遲到資料同批；同時測試新檔回滾與舊 gzip 截斷。
        send(self.store, 'a', [event(number=2, local=started+2000000), event(number=3, local=started)], 2)
        self.store.seal(); self.manifest(started)
        with patch.object(Exporter, 'commit', side_effect=OSError('crash before cursor')):
            with self.assertRaises(OSError): Exporter(self.root, target).run()
        compress_ready(self.root)
        self.assertFalse(list((self.root/'raw').glob('*.jsonl')))
        self.assertEqual(Exporter(self.root, target).run()['seq'], 3)
        out = self.root/'replay'; export_channels(self.root, out)
        for path in target.rglob('*.csv.gz'):
            plain = out/self.root.name/'channels'/path.relative_to(target).with_suffix('')
            self.assertEqual(gzip.decompress(path.read_bytes()), plain.read_bytes())
        with patch.object(Path, 'rglob', side_effect=AssertionError('historical scan')):
            self.assertEqual(Exporter(self.root, target).run()['seq'], 3)

    def test_live_export_and_exclusive_target(self):
        # 已落盤但未封段的資料留給下次；空目錄以外、其他寫入者及其他批次必須拒絕。
        started = 1704067199000000
        self.manifest(started)
        path = self.root / 'manifest.json'
        manifest = json.loads(path.read_text())
        manifest.update(status='running')
        path.write_text(json.dumps(manifest))
        target = self.root / 'export'
        send(self.store, 'a', [event(local=started)])
        self.assertEqual(Exporter(self.root, target).run()['seq'], 0)
        self.store.seal()
        self.assertEqual(Exporter(self.root, target).run()['seq'], 1)
        with (target / '.export.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            with self.assertRaises(BlockingIOError): Exporter(self.root, target).run()
        manifest['started_us'] += 1
        path.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError): Exporter(self.root, target).run()
        other = self.root / 'old'; other.mkdir()
        file = other / 'trade.csv'; file.write_text('existing')
        with self.assertRaises(FileExistsError): Exporter(self.root, other).run()
        self.assertEqual(file.read_text(), 'existing')

    def test_sealed_backup_without_sqlite(self):
        # 唯讀備份僅有 manifest 與 JSONL 也可重建；宣告多一筆須揭露截斷。
        import shutil
        started = 1704067199000000
        send(self.store, 'a', [event(local=started)])
        self.store.seal(); self.manifest(started)
        backup = self.root / 'backup'
        shutil.copytree(self.root / 'raw', backup / 'raw')
        shutil.copy2(self.root / 'manifest.json', backup / 'manifest.json')
        target = self.root / 'csv'
        self.assertEqual(Exporter(backup, target).run()['rows'], 1)
        path = backup / 'manifest.json'
        manifest = json.loads(path.read_text())
        manifest['raw_records'] += 1
        path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, 'truncated'):
            Exporter(backup, target).run()

    def test_binary_keys_and_legacy_text_store(self):
        # 新索引壓緊雜湊，非典型原生字串保留；舊版文字資料庫繼續辨識同一副本。
        MergeStore = importlib.import_module("01_collector.delivery").MergeStore
        database = importlib.import_module("01_collector.delivery").database
        original = event()
        send(self.store, 'a', [original])
        types = self.store.db.execute('SELECT typeof(feed),typeof(ident),length(digest) FROM seen').fetchone()
        self.assertEqual(types, ('integer', 'blob', 32))
        self.assertEqual(self.store.packed_key('AB' * 16), 'AB' * 16)
        self.assertEqual(self.store.packed_key('0 ' * 16), '0 ' * 16)
        root = self.root / 'legacy'
        db = database(root / '_state/merge.sqlite')
        db.execute('CREATE TABLE seen(seq INTEGER PRIMARY KEY,feed TEXT,ident TEXT,digest TEXT,strong INTEGER,received INTEGER)')
        db.close()
        old = MergeStore(root, self.config)
        try:
            send(old, 'a', [original]); send(old, 'b', [original])
            self.assertEqual(old.db.execute('SELECT count(*),typeof(feed),length(digest) FROM seen').fetchone(), (1, 'text', 64))
            self.assertEqual(old.observed_feeds(), {'binance-futures/BTCUSDT/trade'})
        finally:
            old.close()

    def test_coverage_current_active_sessions(self):
        # 兩路有收訊與同一原文已落盤分開計時，替換 session 後不得沿用舊證據。
        monitor = Monitor(self.store, self.config)
        name = 'binance-futures/BTCUSDT/trade'; now = time.time()
        feed = dict(exchange='binance-futures', symbol='BTCUSDT', channel='trade')
        peers = ['a', 'b']; states = {}
        for peer in peers:
            states[peer] = dict(reported_at=now, feeds={name: dict(session=peer, connected=True,
                                subscribed=True, first_data_us=int(now*1e6))})
            send(self.store, peer, [dict(event(number=n), session=peer) for n in range(3)])
        self.assertEqual(monitor.coverage_state(feed, peers, {'a':states['a']}, now)['state'], 'single_path')
        matched = monitor.coverage_state(feed, peers, states, now)
        self.assertEqual(matched['state'], 'matched')
        monitor.coverage[name] = matched
        states['a']['feeds'][name]['session'] = 'different'
        self.assertEqual(monitor.coverage_state(feed, peers, states, now)['state'], 'awaiting_match')
        states['b']['reported_at'] = 0
        self.assertEqual(monitor.coverage_state(feed, peers, states, now)['paths'], 1)


class QueueAndCapacityTests(unittest.TestCase):
    """佇列容量與初始化進度在失敗／重送後仍保持正確。"""
    def test_ack_counts_bytes_only_in_confirmed_prefix(self):
        # 中文原文用位元組計量；確認第一筆後，剩餘大小與原文完全一致。
        with tempfile.TemporaryDirectory() as root:
            spool = Spool(root, dict(DEFAULTS, min_free_gib=.00001, batch_max_bytes=600))
            for n in range(10): spool.emit(dict(event(number=n), note='測試原文'))
            spool.close(); self.assertLess(len(spool.pending()), 10)
            queries = []; spool.db.set_trace_callback(queries.append); spool.acknowledge(1)
            self.assertTrue(all('WHERE seq<=' in q for q in queries if 'sum(' in q))
            size = spool.db.execute('SELECT sum(length(cast(payload AS BLOB))) FROM items').fetchone()[0]
            self.assertEqual(spool.bytes, size); spool.db.close()

    def test_warmup_budget_and_failed_snapshot_priority(self):
        # 五百商品的初始化窗口可預先計算，失敗快照不得變成低優先例行任務。
        value = config(); value['market_settings'] = {'binance-futures': dict(symbols=[f'T{n}USDT' for n in range(501)], channels=['depthSnapshot','openInterest'])}
        with tempfile.TemporaryDirectory() as root:
            scheduler = Schedule(Path(root)/'s.sqlite', value)
            row = next(r for r in scheduler.capacity_summary() if r['channel']=='depthSnapshot')
            self.assertEqual((row['initial_per_minute'], row['warming_budget_windows']), (12,42))
            with scheduler.db: scheduler.db.execute("UPDATE jobs SET due=0 WHERE channel='depthSnapshot'")
            job = scheduler.claim('a', lambda m,s,c: c=='depthSnapshot'); scheduler.finish('a',job,False)
            self.assertEqual(scheduler.db.execute('SELECT priority FROM jobs WHERE id=?',(job['id'],)).fetchone()[0],1)
            self.assertEqual(scheduler.db.execute('SELECT count(*) FROM progress').fetchone()[0],0)
            scheduler.close()

    def test_501_symbols_two_dispatch_cycles(self):
        # 實際派送兩輪五百零一商品，驗證例行與初始化預算分開且不飢餓。
        from collections import Counter
        value = config(); value['market_settings'] = {'binance-futures': dict(symbols=[f'T{n}USDT' for n in range(501)], channels=['depthSnapshot','openInterest'])}
        with tempfile.TemporaryDirectory() as root:
            scheduler = Schedule(Path(root)/'s.sqlite', value); clock = [1000000.0]
            with scheduler.db: scheduler.db.execute('UPDATE jobs SET due=?',(clock[0],))
            with patch.object(scheduler,'now',side_effect=lambda:clock[0]):
                for cycle in range(2):
                    counts = Counter(); clock[0] = 1000000.0 + cycle*61
                    while True:
                        job = scheduler.claim('a')
                        if not job: break
                        counts[job['channel']] += 1; scheduler.finish('a',job,True)
                    self.assertEqual(counts['openInterest'],501)
                    self.assertEqual(counts['depthSnapshot'],12)
                    regular = scheduler.db.execute("SELECT sum(weight) FROM usage WHERE scope='rest:binance-futures' AND regular=1").fetchone()[0]
                    self.assertLessEqual(regular,840)
            scheduler.close()

    def test_expired_lease_still_updates_ip_cooldown(self):
        # 舊請求的 429 仍影響同出口；不得用它完成另一個 worker 的新租約。
        with tempfile.TemporaryDirectory() as root:
            scheduler = Schedule(Path(root)/'s.sqlite', config())
            job = scheduler.claim('a')
            with scheduler.db: scheduler.db.execute("UPDATE jobs SET owner='b',request_id='new' WHERE id=?",(job['id'],))
            scheduler.finish('a',job,False,cooldown=30)
            self.assertIsNone(scheduler.claim('c', lambda market, symbol, channel: market == job['market']))
            self.assertEqual(scheduler.db.execute('SELECT owner FROM jobs WHERE id=?',(job['id'],)).fetchone()[0],'b')
            self.assertEqual(scheduler.db.execute('SELECT count(*) FROM progress').fetchone()[0],0)
            scheduler.close()


class ScheduleAsyncTests(unittest.IsolatedAsyncioTestCase):
    """同步配額鎖等待不得凍結收訊事件迴圈。"""
    async def test_transient_route_failure_is_worker_local(self):
        # 拒絕連線不撤銷共用路由證據；只有原生來源探測失敗才更新共用診斷。
        from unittest.mock import AsyncMock
        target = SimpleNamespace(route_cooldowns={},schedule=SimpleNamespace(call=AsyncMock()))
        group = dict(id='g')
        await Worker.route_failed(target,group,'url',ConnectionError('refused'),0)
        target.schedule.call.assert_not_awaited()
        self.assertIn(('g','url'),target.route_cooldowns)
        await Worker.route_failed(target,group,'url',TimeoutError('route_probe_no_matching_data'),0)
        target.schedule.call.assert_awaited_once_with('route_result',group,'url',False)

    async def test_slow_database_call_does_not_block_loop(self):
        # 背景資料庫操作等待期間，事件迴圈仍可執行心跳。
        with tempfile.TemporaryDirectory() as root:
            scheduler = Schedule(Path(root)/'s.sqlite', config())
            with patch.object(scheduler, 'capacity', side_effect=lambda _: time.sleep(.15)):
                task = asyncio.create_task(scheduler.call('capacity','binance-futures'))
                started = time.monotonic(); await asyncio.sleep(.02)
                self.assertLess(time.monotonic()-started,.1); self.assertFalse(task.done()); await task
            scheduler.close()
