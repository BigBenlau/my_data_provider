"""雙副本框架的固定事件、崩潰邊界、配額與交接驗收。"""
import asyncio
import gzip
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import adapter, atomic_json
from redundancy import group_config, identity, DEFAULTS, configured_feeds, feed_groups
from delivery import Spool, MergeStore, read_packet, write_packet
from replica_protocol import atoms, classify_group, subscription
from replica_monitor import Monitor
from rest_schedule import Schedule
from channel_export import Exporter
from raw import read_records, export_channels, StorageError


def config(family='binance'):
    # 縮小固定測試的商品集合，不接觸使用者設定或既有輸出。
    value = group_config(family)
    for market, settings in value['market_settings'].items(): settings['symbols'] = [settings['symbols'][0]]
    value['redundancy'].update(min_free_gib=.00001, flush_seconds=.01, heartbeat_seconds=.2, drain_timeout=2)
    return value


def event(channel='trade', number=1, market='binance-futures', local=None):
    # 建立精確字串價量與原生序號，時間與接收時間可獨立調整。
    payload = dict(e='trade', s='BTCUSDT', t=number, p='100.000001', q='2', T=1000 + number, E=1000 + number, m=False)
    if channel.startswith('depth'):
        payload = dict(e='depthUpdate', s='BTCUSDT', E=1000 + number, T=1000 + number,
                       U=number, u=number, pu=number - 1, b=[['99', '2']], a=[['101', '3']])
    if channel == 'ticker': payload = dict(e='24hrTicker', s='BTCUSDT', E=number, c='100')
    return dict(kind='ws', exchange=market, symbol='BTCUSDT', channel=channel, time_unit='ms', session='s',
                local_timestamp=local or time.time_ns() // 1000, raw=json.dumps({'stream': 'btcusdt@' + channel, 'data': payload}))


def send(store, worker, records, start=1):
    # 輸入序號屬於 worker；每筆行情原生 ID 另行保存，不混用兩者。
    return store.ingest(worker, [dict(seq=n, record=r) for n, r in enumerate(records, start)])


class MergeTests(unittest.TestCase):
    """確認事件身分、遲到資料、批次及可恢復提交。"""
    def setUp(self):
        # 每個測試以獨立本地磁碟資料庫驗證真正的提交與重啟。
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.config = config(); self.store = MergeStore(self.root, self.config)

    def tearDown(self):
        # 先釋放 SQLite 與檔案，再清理測試暫存。
        if self.store: self.store.close()
        self.temp.cleanup()

    def test_copies_retransmit_and_late_new_events(self):
        # ACK 遺失時重送不增列；較晚到的不同原生事件仍保存。
        a = event(number=2); send(self.store, 'a', [a]); send(self.store, 'b', [a])
        self.assertEqual(send(self.store, 'a', [a]), 1)
        send(self.store, 'b', [event(number=1)], 2)
        self.assertEqual(self.store.db.execute('SELECT written FROM output').fetchone()[0], 2)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM outbox').fetchone()[0], 0)

    def test_conflicting_price_is_retained_and_blocks_handover(self):
        # 相同強身分不同內容不能覆寫或丟棄，持久保存衝突證據。
        original = event(); changed = dict(original, raw=original['raw'].replace('100.000001', '100.000002'))
        send(self.store, 'a', [original]); send(self.store, 'b', [changed])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM seen').fetchone()[0], 2)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM conflicts').fetchone()[0], 1)

    def test_weak_occurrences_do_not_collapse_equal_values(self):
        # 無原生強身分時跨路亦不能證明重複，寧可保留四筆並註記歧義。
        item = event('ticker'); send(self.store, 'a', [item, item]); send(self.store, 'b', [item, item])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM seen').fetchone()[0], 4)
        send(self.store, 'b', [dict(item, local_timestamp=item['local_timestamp'] + 6000000)], 3)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM seen').fetchone()[0], 5)

    def test_partial_batch_overlap_preserves_new_trade(self):
        # Bybit 同 seq 內多成交及兩批部分重疊，只消除逐筆 i 相同事件。
        base = dict(kind='ws', exchange='bybit', symbol='BTCUSDT', channel='publicTrade', session='s', local_timestamp=1000000)
        rows = [dict(i=str(n), s='BTCUSDT', p='100', v='1', seq=1) for n in range(3)]
        left = dict(base, raw=json.dumps(dict(topic='publicTrade.BTCUSDT', type='snapshot', data=rows[:2])))
        right = dict(base, raw=json.dumps(dict(topic='publicTrade.BTCUSDT', type='snapshot', data=rows[1:])))
        send(self.store, 'a', [left]); send(self.store, 'b', [right]); self.store.seal()
        records = [json.loads(line) for p in (self.root / 'raw').glob('*.jsonl') for line in p.read_text().splitlines()]
        self.assertEqual(len(records), 3)
        self.assertTrue(all(len(json.loads(r['raw'])['data']) == 1 and r['batch_split_version'] == 1 for r in records))

    def test_restart_between_index_commit_and_jsonl_commit(self):
        # 索引已提交但 JSONL 失敗時不得 ACK；重啟重寫 outbox 後才確認。
        with patch.object(self.store, 'flush', side_effect=OSError('disk failure')):
            with self.assertRaises(StorageError): send(self.store, 'a', [event()])
        self.store.db.close(); self.store = MergeStore(self.root, self.config)
        self.assertEqual(send(self.store, 'a', [event()]), 1)
        self.assertEqual(self.store.db.execute('SELECT written FROM output').fetchone()[0], 1)

    def test_uncommitted_tail_removed_on_recovery(self):
        # JSONL fsync 後資料庫提交失敗的尾端，重播時只能出現一次。
        send(self.store, 'a', [event()])
        path, offset = self.store.db.execute('SELECT path,offset FROM output').fetchone()
        with (self.root / path).open('ab') as handle: handle.write(b'{"seq":999}\n')
        self.store.db.close(); self.store = MergeStore(self.root, self.config)
        send(self.store, 'a', [event(number=2)], 2); self.store.seal()
        records = [json.loads(line) for p in (self.root / 'raw').glob('*.jsonl') for line in p.read_text().splitlines()]
        self.assertEqual([r['seq'] for r in records], [1, 2])

    def test_gap_covered_by_peer_and_uncovered_gap(self):
        # 另一副本的完整 pu 鏈可證明覆蓋；較大的終點本身不足以證明。
        send(self.store, 'a', [event('depth@0ms', 1), event('depth@0ms', 3)])
        send(self.store, 'b', [event('depth@0ms', n) for n in (1, 2, 3)])
        monitor = Monitor(self.store, self.config); self.assertEqual(monitor.resolve_gaps(), [])
        self.assertEqual(self.store.db.execute('SELECT resolved FROM gaps').fetchone()[0], 1)
        send(self.store, 'a', [event('depth@0ms', 6)], 3)
        with self.store.db: self.store.db.execute('UPDATE gaps SET created=0 WHERE resolved=0')
        self.assertEqual(monitor.resolve_gaps(), ['binance-futures/BTCUSDT/depth@0ms'])

    def test_csv_replay_recovery_and_utc_day(self):
        # CSV 游標失敗後截回偏移；跨 UTC 日及離線重播得到同一位元組。
        started = 1704067199000000; self.config['market_settings'] = {'binance-futures': {'symbols': ['BTCUSDT'], 'channels': ['trade', 'forceOrder']}}
        self.config['markets'] = ['binance-futures']
        send(self.store, 'a', [event(local=started), event(number=2, local=started + 2000000)])
        self.store.seal(); self.manifest(started)
        target = self.root / 'export'
        first = Exporter(self.root, target).run()
        self.assertEqual(first, dict(seq=2, rows=2, files=4))
        # 上次同步過的輸出若有未提交尾端，也要復原既有的位元組。
        path = next(target.rglob('trade.csv.gz'))
        with path.open('ab') as handle: handle.write(b'uncommitted')
        self.assertEqual(Exporter(self.root, target).run(), first)
        out = self.root / 'replay'; export_channels(self.root, out)
        for path in target.rglob('*.csv.gz'):
            plain = out / self.root.name / 'channels' / path.relative_to(target).with_suffix('')
            self.assertEqual(gzip.decompress(path.read_bytes()), plain.read_bytes())
        self.assertEqual(len(list(target.rglob('forceOrder.csv.gz'))), 2)

    def manifest(self, started):
        # 建立 reader 所需的封存資訊，不替實際資料完整性背書。
        count = self.store.db.execute('SELECT written FROM output').fetchone()[0]
        atomic_json(self.root / 'manifest.json', dict(schema_version=3, status='stopped', raw_records=count,
                    config=self.config, started_us=started, run_id=self.root.name))


class SpoolTests(unittest.TestCase):
    """確認 worker 暫存只在持久 ACK 後清理。"""
    def test_durable_resend_then_cleanup(self):
        # 模擬 worker 程序重建，保留原序號；確認後不留下 payload 資料集。
        with tempfile.TemporaryDirectory() as root:
            options = dict(DEFAULTS, min_free_gib=.00001, flush_seconds=.001)
            spool = Spool(root, options); spool.emit(event()); spool.close(); spool.db.close()
            spool = Spool(root, options)
            self.assertEqual(spool.pending()[0]['seq'], 1)
            with self.assertRaises(ValueError): spool.acknowledge(2)
            spool.acknowledge(1); self.assertEqual(spool.pending(), [])
            spool.emit(event(number=2)); spool.close()
            self.assertEqual(spool.pending()[0]['seq'], 2); spool.db.close()

    def test_capacity_and_disk_failure_stop(self):
        # 容量不足或寫入錯誤不得假 ACK 或清掉舊資料。
        with tempfile.TemporaryDirectory() as root:
            spool = Spool(root, dict(DEFAULTS, min_free_gib=.00001, spool_max_bytes=1))
            spool.emit(event())
            with self.assertRaises(StorageError): spool.close()
            self.assertEqual(spool.ack, 0); spool.db.close()


class AdapterTests(unittest.TestCase):
    """五家協議分開驗證 ACK、心跳、身分及序號重置。"""
    def test_asset_groups_across_markets_and_channels(self):
        # 逐市場核對原生商品歸屬，包含 Upbit 反向報價、衍生品及容易誤判的名稱。
        products = {
            'binance': ('BTCUSDT', 'ETHUSDT', 'ETHFIUSDT', 'SOLUSDT'),
            'bybit': ('BTCUSDT', 'ETHBTC', 'BTCDOMUSDT', 'SOLUSDT'),
            'okx': ('BTC-USDT', 'ETH-USDT', 'ETHFI-USDT', 'SOL-BTC'),
            'coinbase': ('BTC-USD', 'ETH-USD', 'ETHFI-USD', 'SOL-BTC'),
            'upbit': ('KRW-BTC', 'BTC-ETH', 'KRW-ETHFI', 'BTC-SOL'),
        }
        for family, symbols in products.items():
            cfg = group_config(family)
            for market, settings in cfg['market_settings'].items():
                suffix = '-SWAP' if market == 'okex-swap' else ''
                settings['symbols'] = [s + suffix for s in reversed(symbols)]
            groups = feed_groups(cfg)
            self.assertEqual(len({g['id'] for g in groups}), len(groups))
            for group in groups:
                market, channel, bucket, index = group['id'].split('/')
                suffix = '-SWAP' if market == 'okex-swap' else ''
                expected = {'BTC': symbols[:1], 'ETH': symbols[1:2], 'others': symbols[2:]}
                self.assertEqual(set(group['lookup']), {s + suffix for s in expected[bucket]})
                self.assertEqual(index, '0')
            # 每個設定的 WS 來源恰好訂閱一次，所有 worker 都得到相同的佈局。
            planned = [(f['exchange'], f['channel'], f['symbol']) for g in groups for f in g['feeds']]
            feeds = [(f['exchange'], f['channel'], f['symbol']) for f in configured_feeds(cfg)]
            self.assertCountEqual(planned, feeds)
            for settings in cfg['market_settings'].values():
                settings['symbols'].reverse()
            self.assertEqual(feed_groups(cfg), groups)

    def test_other_group_capacity_and_new_products(self):
        # 同時檢查設定容量及 Bybit 現貨上限，新增商品不能擠進 BTC 或 ETH。
        cfg = group_config('bybit')
        settings = cfg['market_settings']['bybit-spot']
        settings.update(channels=['publicTrade'], symbols=['BTCUSDT', 'ETHUSDT', 'BTCXYZ'])
        settings['base_assets'] = {'BTCXYZ': 'BTC'}
        settings['symbols'] += [f'ALT{i:02}USDT' for i in range(21)]
        for size, expected in [(100, [10, 10, 1]), (7, [7, 7, 7])]:
            cfg['redundancy']['group_size'] = size
            groups = [g for g in feed_groups(cfg) if g['id'].startswith('bybit-spot/')]
            self.assertEqual([len(g['feeds']) for g in groups if '/others/' in g['id']], expected)
            self.assertEqual(set(groups[0]['lookup']), {'BTCUSDT', 'BTCXYZ'})
            self.assertCountEqual([s for g in groups for s in g['lookup']], settings['symbols'])

    def test_ack_and_control_by_exchange(self):
        # 各自訂閱格式與 ACK 判斷不可互相借用。
        acks = {'binance': {'result': None, 'id': 1}, 'bybit': {'op': 'subscribe', 'success': True},
                'okx': {'event': 'subscribe', 'arg': {'channel': 'trades', 'instId': 'BTC-USDT'}},
                'coinbase': {'type': 'subscriptions', 'channels': [{'name': 'matches', 'product_ids': ['BTC-USD']}]}}
        for family in ('binance', 'bybit', 'okx', 'coinbase', 'upbit'):
            groups = feed_groups(config(family)); self.assertTrue(all(subscription(g) for g in groups))
            if family == 'upbit': self.assertFalse(adapter('upbit').requires_ack); continue
            matches = [g for g in groups if classify_group(g, acks[family])[0] == 'ack']
            self.assertTrue(matches, family)
            self.assertEqual(classify_group(groups[0], {'op': 'pong'})[0], 'control')

    def test_native_identity_and_resets(self):
        # seq 重置依各自 adapter 判斷，不把 Bybit u 當成 Binance pu。
        okx = adapter('okex').replica_identity('books', {'seqId': 1, 'prevSeqId': -1, 'ts': '5'}, {'action': 'snapshot'})
        bybit = adapter('bybit').replica_identity('orderbook.50', {'u': 1, 'seq': 2}, {'type': 'delta', 'cts': 3})
        self.assertTrue(okx[2]['reset']); self.assertTrue(bybit[2]['reset']); self.assertIsNone(bybit[2]['previous'])
        self.assertTrue(adapter('upbit').replica_identity('trade', {'sequential_id': 42, 'type': 'trade'}, {})[1])
        self.assertFalse(adapter('coinbase').replica_identity('level2_batch', {'type': 'l2update', 'time': 't'}, {})[1])

    def test_runtime_identity_excludes_worker_id(self):
        # A/B/C 共用設定及程式版本；可修改 runtime 身分但不可更動採集雜湊。
        value = config(); identities = [identity(value, 'sha256:image') for _ in range(3)]
        self.assertEqual(identities[0], identities[1]); self.assertEqual(identities[1], identities[2])
        other = dict(value, config_hash='different')
        self.assertNotEqual(identity(other, 'sha256:image'), identities[0])

    def test_connection_rate_limit_is_shared_between_workers(self):
        # OKX 的每秒上限跨市場與副本共用，不能只限制五分鐘總量。
        value = config('okx')
        with tempfile.TemporaryDirectory() as root:
            a = Schedule(Path(root) / 's.sqlite', value); b = Schedule(Path(root) / 's.sqlite', value)
            self.assertTrue(a.connection('okex')); self.assertTrue(b.connection('okex-swap'))
            self.assertFalse(a.connection('okex')); a.close(); b.close()

    def test_periodic_snapshot_same_update_id_is_not_same_event(self):
        # Bybit level1 三秒重發可以沿用 u，發布時間不同不能去重掉原生快照。
        rule = adapter('bybit').replica_identity
        data = {'u': 7, 'seq': 10, 'b': [['99', '2']], 'a': [['101', '1']]}
        first = rule('orderbook.1', data, {'type': 'snapshot', 'cts': 5, 'ts': 1000})
        later = rule('orderbook.1', data, {'type': 'snapshot', 'cts': 5, 'ts': 4000})
        self.assertNotEqual(first[0], later[0]); self.assertTrue(first[1])
        self.assertFalse(rule('orderbook.1', data, {'type': 'snapshot'})[1])


class CapacityTests(unittest.TestCase):
    """大量商品、共享租約及限流視窗不得隨 worker 更換重置。"""
    def test_two_full_cycles_of_501_symbols_keep_budget_partitions(self):
        # 實際模擬兩輪五百零一個 OI 派送，初始化快照不得擠掉例行容量。
        value = config(); value['market_settings'] = {'binance-futures': dict(symbols=[f'T{i}USDT' for i in range(501)], channels=['depthSnapshot', 'openInterest'])}
        with tempfile.TemporaryDirectory() as root:
            scheduler = Schedule(Path(root) / 's.sqlite', value)
            with scheduler.db: scheduler.db.execute('UPDATE jobs SET due=0')
            for now in (1000, 1061):
                channels = []; jobs = []
                with patch.object(scheduler, 'now', return_value=now):
                    for index in range(2000):
                        owner = 'a' if index % 2 else 'b'; job = scheduler.claim(owner)
                        if not job: break
                        channels.append(job['channel']); jobs.append(job['id'])
                        scheduler.finish(owner, job, True)
                self.assertEqual(channels.count('openInterest'), 501)
                self.assertEqual(channels.count('depthSnapshot'), 12)
                self.assertEqual(len(jobs), len(set(jobs)))
                usage = dict(scheduler.db.execute('SELECT regular,sum(weight) FROM usage WHERE scope=? GROUP BY regular', ('rest:binance-futures',)))
                self.assertLessEqual(usage.get(1, 0), 840); self.assertLessEqual(usage.get(0, 0), 240)
            scheduler.close()

    def test_500_symbols_capacity_and_persistent_leases(self):
        # 五百商品均勻分期；OI 與深度總權重不超過七成例行容量。
        value = config(); value['market_settings'] = {'binance-futures': dict(symbols=[f'T{i}USDT' for i in range(501)], channels=['depthSnapshot', 'openInterest'])}
        with tempfile.TemporaryDirectory() as root:
            a = Schedule(Path(root) / 'schedule.sqlite', value); b = Schedule(Path(root) / 'schedule.sqlite', value)
            oi = a.interval('binance-futures', 'openInterest'); depth = a.interval('binance-futures', 'depthSnapshot')
            summary = {r['channel']: r for r in a.capacity_summary()}
            self.assertEqual(summary['openInterest']['symbols'], 501)
            self.assertEqual(summary['openInterest']['interval'], oi)
            from supervisor import Supervisor
            from types import SimpleNamespace
            target = SimpleNamespace(config=value, schedule=a, capacity_state=None)
            with self.assertLogs('market-data', level='WARNING') as logs: Supervisor.capacity_log(target)
            self.assertTrue(any('effective_s=60' in line and 'symbols=501' in line for line in logs.output))
            self.assertLessEqual(501 * 60 / oi + 501 * 20 * 60 / depth, 1200 * .7)
            self.assertEqual(a.db.execute('SELECT count(*) FROM jobs').fetchone()[0], 1003)
            first = a.claim('a'); second = b.claim('b'); self.assertNotEqual(first['id'], second['id'])
            a.finish('a', first, False, cooldown=30)
            self.assertIsNone(b.claim('b')); a.close(); b.close()

    def test_raw_request_windows_and_header_reset(self):
        # 除權重外也限制請求次數；新分鐘回報不得沿用上一分鐘的 used。
        value = config(); value['market_settings'] = {'binance': dict(symbols=['BTCUSDT'], channels=['depthSnapshot'])}
        with tempfile.TemporaryDirectory() as root:
            scheduler = Schedule(Path(root) / 's.sqlite', value)
            scheduler.metadata('binance', {'symbols': [{'symbol': 'BTCUSDT'}], 'rateLimits': [
                dict(rateLimitType='REQUEST_WEIGHT', interval='MINUTE', intervalNum=1, limit=6000),
                dict(rateLimitType='RAW_REQUESTS', interval='SECOND', intervalNum=1, limit=1)]})
            self.assertIsNone(scheduler.claim('a'))
            scheduler.close()


class HandoverTests(unittest.TestCase):
    """安全交接只能依目前 session 的實際共同資料。"""
    setUp = MergeTests.setUp
    tearDown = MergeTests.tearDown
    def test_verified_candidate_and_silent_candidate(self):
        # 至少三個共同事件加觀察時間；靜默候選不會取代舊路。
        self.config['market_settings'] = {'binance-futures': {'symbols': ['BTCUSDT'], 'channels': ['trade']}}
        self.config['markets'] = ['binance-futures']; monitor = Monitor(self.store, self.config)
        since = time.time() - 11
        for worker in ('a', 'b', 'c'):
            send(self.store, worker, [event(number=n) for n in range(1, 4)])
            monitor.status(worker, dict(feeds={'binance-futures/BTCUSDT/trade': dict(connected=True, subscribed=True, session='s')}))
        self.assertTrue(monitor.verify('c', ['a', 'b'], since)[0])
        self.assertFalse(monitor.verify('d', ['a', 'b'], since)[0])
        monitor.status('c', dict(feeds={'binance-futures/BTCUSDT/trade': dict(connected=True, subscribed=True, session='new')}))
        self.assertFalse(monitor.verify('c', ['a', 'b'], since)[0])

    def test_both_sessions_reconnect_does_not_hide_gap(self):
        # 兩路都換 session 後仍以合併序號發現未覆蓋範圍。
        send(self.store, 'a', [event('depth@0ms', 1)])
        send(self.store, 'b', [event('depth@0ms', 1)])
        send(self.store, 'a', [dict(event('depth@0ms', 5), session='new')], 2)
        rows = self.store.db.execute('SELECT worker,start_u,end_u FROM gaps').fetchall()
        self.assertIn(('__merged__', 1, 5), rows)

    def test_merged_snapshot_mapping_after_worker_switch(self):
        # 第三版只用邏輯 session 與請求邊界，副本切換不清掉有效訂單簿。
        path = Path(__file__).resolve().parents[1] / '02_normalize_tardis.py'
        spec = importlib.util.spec_from_file_location('mapped_replica_test', path)
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        db = sqlite3.connect(':memory:'); mapper = module.Mapper(db)
        first = dict(event('depth@0ms', 10, local=1100000), merged=True, session='merged')
        self.assertEqual(mapper.consume(first), [])
        snapshot = dict(kind='rest', exchange='binance-futures', symbol='BTCUSDT', channel='depthSnapshot',
                        session='merged', merged=True, local_timestamp=1200000, request_started_us=1000000,
                        generation=1, time_unit='ms', raw=json.dumps(dict(lastUpdateId=9, bids=[['99', '1']], asks=[['101', '1']])))
        rows = mapper.consume(snapshot); self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1]['local_timestamp'], 1200000)
        self.assertEqual(rows[0][1]['bid_amount'], 2)
        self.assertEqual(mapper.consume(dict(event('depth@0ms', 12), session='merged', merged=True)), [])
        self.assertGreater(mapper.issues['sequence_gap'], 0); db.close()


class DockerRuntimeTests(unittest.IsolatedAsyncioTestCase):
    """容器 API 的映像與命令一致性可在沒有 daemon 時先做固定驗證。"""
    async def test_image_mismatch_refused_and_same_entrypoint(self):
        # 不同映像的既有容器不可接管；所有 worker 只改執行期名稱。
        from container_runtime import Runtime
        from unittest.mock import AsyncMock
        with tempfile.TemporaryDirectory() as root:
            value = config(); runtime = Runtime(root, value, 'docker', 'tag')
            runtime.image = 'sha256:expected'
            runtime.api = AsyncMock(return_value={'Id': 'container', 'Image': 'sha256:other'})
            with self.assertRaises(ValueError): await runtime.start_container('a')
            left = runtime.command('a', '/data'); right = runtime.command('b', '/data')
            self.assertEqual([x for x in left if x != 'a'], [x for x in right if x != 'b'])


class LocalRuntime:
    """測試直接啟動真正 Worker，只替換容器程序管理。"""
    def __init__(self, root, value):
        # 所有副本仍使用同一個 Worker 類別與同一份採集設定。
        self.root = root; self.config = value; self.image = 'fixture-image'; self.tasks = {}

    async def alive(self, worker):
        # 回報真實非同步任務狀態，不使用固定的健康答案。
        return worker in self.tasks and not self.tasks[worker].done()

    async def start(self, worker):
        # 生命週期與交付都走正式程式，只有外部行情使用固定伺服器。
        from worker import Worker
        instance = Worker(self.root, self.config, worker, self.image)
        self.tasks[worker] = asyncio.create_task(instance.execute())

    async def stop(self, worker):
        # 測試結束回收所有任務並揭露失敗，不留下背景採集。
        task = self.tasks.get(worker)
        if not task: return
        if not task.done(): task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    """真正 Unix IPC、收訊與 supervisor 重啟的整合測試。"""
    async def websocket(self, request):
        # 用五家不同原生格式回傳資料，第一條連線中斷時其他路仍接收。
        from aiohttp import web
        ws = web.WebSocketResponse(); await ws.prepare(request); await ws.receive_json()
        self.connections += 1; number = self.connections
        market = request.match_info['market']
        ack, payload = self.fixture(market, 1)
        if ack: await ws.send_json(ack)
        try:
            for n in range(1, 100):
                _, payload = self.fixture(market, n)
                await ws.send_json(payload)
                if number == 1 and n == 5: await ws.close(); break
                await asyncio.sleep(.03)
        except (ConnectionError, OSError): pass
        return ws

    def fixture(self, market, n):
        # 原生事件時間固定於事件 ID，讓副本真正代表同一個市場事件。
        if market == 'binance-futures': return {'result': None, 'id': 1}, json.loads(event(number=n)['raw'])
        if market == 'bybit': return {'op': 'subscribe', 'success': True}, dict(topic='publicTrade.BTCUSDT', data=[dict(i=str(n), s='BTCUSDT', p='100', v='1')])
        if market == 'okex':
            arg = dict(channel='trades', instId='BTC-USDT')
            return dict(event='subscribe', arg=arg), dict(arg=arg, data=[dict(tradeId=str(n), px='100', sz='1', ts=str(n))])
        if market == 'coinbase':
            return dict(type='subscriptions', channels=[dict(name='matches', product_ids=['BTC-USD'])]), dict(type='match', product_id='BTC-USD', trade_id=n, price='100', size='1')
        return None, dict(type='trade', code='KRW-BTC', sequential_id=n, trade_price=100, trade_volume=1, stream_type='REALTIME')

    def settings(self, market, port):
        # REST 從此測試排除，配額另有固定驗收；每市場仍走自己的訂閱及解析。
        family = {'binance-futures': 'binance', 'okex': 'okx'}.get(market, market)
        value = config(family)
        channel = {'bybit': 'publicTrade', 'okex': 'trades', 'coinbase': 'matches'}.get(market, 'trade')
        symbol = {'okex': 'BTC-USDT', 'coinbase': 'BTC-USD', 'upbit': 'KRW-BTC'}.get(market, 'BTCUSDT')
        value['market_settings'] = {market: dict(symbols=[symbol], channels=[channel])}; value['markets'] = [market]
        value['redundancy'].update(rotation_seconds=600, candidate_timeout=.1, verify_seconds=.01)
        feed = dict(exchange=market, symbol=symbol, channel=channel, time_unit='ms', urls=[f'http://127.0.0.1:{port}/{market}'])
        return value, feed

    async def start_fixture(self):
        # 使用系統分配的本機埠，不依賴任何外部交易所或帳戶。
        from aiohttp import web
        self.connections = 0; app = web.Application(); app.router.add_get('/{market}', self.websocket)
        runner = web.AppRunner(app); await runner.setup()
        site = web.TCPSite(runner, '127.0.0.1', 0); await site.start()
        return runner, site._server.sockets[0].getsockname()[1]

    async def test_five_markets_dual_workers_and_candidate_rejection(self):
        # 五個 adapter 共用同一實際框架，單路斷線及失格候選不停止另一條。
        from supervisor import Supervisor
        runner, port = await self.start_fixture()
        try:
            for market in ('binance-futures', 'bybit', 'okex', 'coinbase', 'upbit'):
                with tempfile.TemporaryDirectory() as root:
                    value, feed = self.settings(market, port); runtime = LocalRuntime(root, value)
                    with patch.object(adapter(market), 'feeds', return_value=[feed]):
                        supervisor = Supervisor(root, value, runtime)
                        supervisor.state['last_rotation'] = 0
                        await supervisor.serve(.7)
                    supervisor.store.close(); supervisor.schedule.close()
                    rows = list(read_records(root)); self.assertGreater(len(rows), 5, market)
                    self.assertEqual(len(rows), len({atoms(r)[0]['ident'] for r in rows}), market)
                    self.assertEqual(list((Path(root) / '_spool').iterdir()), [])
        finally: await runner.cleanup()

    async def test_supervisor_restart_workers_buffer_then_replay(self):
        # 主管停止服務時 worker 繼續落盤；新主管接管後消除重送並清理緩衝。
        from supervisor import Supervisor
        runner, port = await self.start_fixture()
        try:
            with tempfile.TemporaryDirectory() as root:
                value, feed = self.settings('binance-futures', port); runtime = LocalRuntime(root, value)
                with patch.object(adapter('binance-futures'), 'feeds', return_value=[feed]):
                    first = Supervisor(root, value, runtime); task = asyncio.create_task(first.serve())
                    await asyncio.sleep(.4); task.cancel(); await asyncio.gather(task, return_exceptions=True)
                    first.store.close(); first.schedule.close()
                    await asyncio.sleep(.3)
                    self.assertTrue(all(not t.done() for t in runtime.tasks.values()))
                    second = Supervisor(root, value, runtime); await second.serve(.5)
                    second.store.close(); second.schedule.close()
                    rows = list(read_records(root)); self.assertGreater(len(rows), 20)
                    self.assertEqual(list((Path(root) / '_spool').iterdir()), [])
        finally: await runner.cleanup()


if __name__ == '__main__': unittest.main()
