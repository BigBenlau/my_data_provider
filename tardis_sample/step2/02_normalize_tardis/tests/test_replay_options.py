"""驗證多程序、缺失資料與有界磁碟暫存的重播邊界。"""
from contextlib import closing
import csv
import gzip
import importlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import unittest

STAGE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(STAGE.parent))
norm = importlib.import_module('02_normalize_tardis.normalization')
MS = 1704067200000


def manifest(root, family, count, **extra):
    # 建立小批次並讓健康等級只反映測試中刻意製造的缺失。
    (root / 'raw').mkdir(parents=True, exist_ok=True)
    data = dict(schema_version=3, status='stopped', started_us=1, raw_records=count,
                config=dict(markets=[family]), health_at_stop=dict(data_quality='healthy'))
    (root / 'manifest.json').write_text(json.dumps(dict(data, **extra)))


def bybit(seq, channel, data, action='snapshot'):
    # 外層及原生時間獨立，以固定時間和微秒窗比較。
    raw = dict(topic=channel+'.BTCUSDT', ts=MS+seq, type=action, data=data)
    return dict(seq=seq, kind='ws', exchange='bybit', symbol='BTCUSDT', channel=channel,
                session='merged', local_timestamp=(MS+seq)*1000, raw=json.dumps(raw))


def trade(family, seq, identity=None):
    # 同一原生 ID 可在不同提交序號重送，成交內容維持不變。
    tid = seq if identity is None else identity
    if family == 'bybit':
        item = dict(s='BTCUSDT', i=str(tid), p='42', v='1', S='Buy', T=MS+tid)
        return bybit(seq, 'publicTrade', [item])
    data = dict(type='trade', code='KRW-BTC', sequential_id=tid, trade_volume=1, trade_price=42,
                ask_bid='ASK', trade_timestamp=MS+tid)
    return dict(seq=seq, kind='ws', exchange='upbit', symbol='KRW-BTC', channel='trade',
                session='merged', local_timestamp=(MS+seq)*1000, raw=json.dumps(data))


def jsonl(records):
    # 允許穿插無效原文，以驗證同段後面的有效行仍可讀取。
    return b''.join((json.dumps(r).encode() if isinstance(r, dict) else r) + b'\n' for r in records)


def tables(path):
    # 逐份比對各市場的實際 CSV，包含只有表頭的檔案。
    return {p.relative_to(path): p.read_bytes() for p in path.rglob('*.csv')}


class ReplayTests(unittest.TestCase):
    def setUp(self):
        # 每個測試擁有獨立的輸入及全新輸出。
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.work = Path(temporary.name)

    def test_missing_segment_bad_line_and_book_restart(self):
        # 資料洞後的增量不能混入舊簿；成交索引、同段後續列與缺失明細都要保留。
        root, out = self.work/'run', self.work/'out'
        manifest(root, 'bybit', 13)
        state = root/'_state'
        state.mkdir()
        with closing(sqlite3.connect(state/'merge.sqlite')) as db:
            db.execute('CREATE TABLE segments(name,first_seq,last_seq)')
            db.executemany('INSERT INTO segments VALUES (?,?,?)', [('1.jsonl',1,3),('4.jsonl',4,5),('6.jsonl',6,13)])
            db.commit()
        book = dict(s='BTCUSDT', u=1, b=[['40','1']], a=[['44','1']])
        (root/'raw/1.jsonl').write_bytes(jsonl([trade('bybit',1), bybit(2,'tickers',dict(symbol='BTCUSDT', lastPrice='42')),
                                                bybit(3,'orderbook.50',book)]))
        incomplete = trade('bybit',11)
        del incomplete['symbol']
        rows = [bybit(6,'orderbook.50',dict(book,u=2),'delta'), trade('bybit',7,1),
                bybit(8,'tickers',dict(symbol='BTCUSDT',markPrice='43')), b'{', trade('bybit',10),
                incomplete, bybit(12,'orderbook.50',book), trade('bybit',13)]
        (root/'raw/6.jsonl.gz').write_bytes(gzip.compress(jsonl(rows)))
        with self.assertLogs('market-data', level='WARNING') as logs:
            report = norm.replay(norm.mapper_for('bybit'), root, out, batch_size=1)
        self.assertIn('4.jsonl', '\n'.join(logs.output))
        self.assertEqual(report['status'], 'completed_with_warnings')
        gaps = [(i['first_seq'],i['last_seq']) for i in report['recovery_warnings'] if i['reason']=='sequence gap']
        self.assertEqual(gaps, [(4,5),(9,9)])
        self.assertTrue(any(i.get('line')==6 and 'symbol' in i.get('detail','') for i in report['recovery_warnings']))
        self.assertEqual(report['mapper_issues'], {'depth_before_snapshot':1, 'duplicate_trades':1})
        with next(out.glob('*/derivative_ticker/*.csv')).open() as handle:
            self.assertEqual([row['last_price'] for row in csv.DictReader(handle)], ['42',''])
        self.assertEqual(sum(n for p,n in report['rows'].items() if '/quotes/' in p), 2)
        self.assertEqual(sum(n for p,n in report['rows'].items() if '/trades/' in p), 3)

    def test_parallel_single_and_open_ended_windows(self):
        # 同一個 CLI 經兩個獨立子程序處理，結果須與單家全量及各種事件窗一致。
        families = ('bybit','upbit')
        source = self.work/'in'
        for family in families:
            manifest(source/family, family, 3)
            (source/family/'raw/1.jsonl').write_bytes(jsonl(trade(family,i) for i in range(1,4)))
        for index, (lower,upper,count) in enumerate(((None,None,3),(MS+2,None,2),(None,MS+2,1),(MS+2,MS+3,1))):
            out = self.work/f'parallel{index}'
            cmd = [sys.executable, str(STAGE/'02_normalize_tardis.py'), '--exchange', *families, '--jobs','2',
                   '--input', str(source),'--output',str(out),'--batch-size','1']
            for flag, value in (('--start-us',lower),('--end-us',upper)):
                if value is not None:
                    cmd.extend([flag,str(value*1000)])
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            self.assertEqual(len(set(re.findall(r'normalization_started exchange=\w+ pid=(\d+)',result.stderr))), 2)
            for family, data in json.loads(result.stdout).items():
                self.assertEqual(data['csv_rows'],count)
                serial = self.work/f'serial{index}{family}'
                norm.replay(norm.mapper_for(family), source/family, serial,
                            start=lower*1000 if lower else None, end=upper*1000 if upper else None)
                self.assertEqual(tables(out/family), tables(serial))

    def test_recovery_stops_at_committed_sequence(self):
        # 即使已提交範圍有洞，也不能將檔案尾端未提交的完整 JSON 行當成有效成交。
        root = self.work/'run'
        manifest(root, 'upbit', 3, status='running')
        (root/'_state').mkdir()
        with closing(sqlite3.connect(root/'_state/merge.sqlite')) as db:
            db.execute('CREATE TABLE output(written)')
            db.execute('INSERT INTO output VALUES (3)')
            db.commit()
        (root/'raw/1.jsonl.part').write_bytes(jsonl(trade('upbit',i) for i in (1,3,4)))
        with self.assertLogs('market-data', level='WARNING'):
            report = norm.replay(norm.mapper_for('upbit'), root, self.work/'out', recover=True)
        self.assertEqual(report['raw_records'], 2)
        self.assertEqual([w['first_seq'] for w in report['recovery_warnings'] if w['reason']=='sequence gap'], [2])

    def test_many_legacy_files_and_bad_gzip_segment(self):
        # 舊檔超過同時開檔上限時用磁碟排序；損壞壓縮段之後仍可讀下一段。
        root = self.work/'legacy'
        manifest(root, 'upbit', 40, schema_version=1, raw_schema_version=1)
        for number in range(40,0,-1):
            (root/'raw'/f'part{40-number}.jsonl').write_text(json.dumps(trade('upbit',number)))
        legacy = norm.replay(norm.mapper_for('upbit'), root, self.work/'old', batch_size=1)
        self.assertEqual(legacy['raw_records'],40)
        with next((self.work/'old').glob('*/trades/*.csv')).open() as handle:
            self.assertEqual([int(r['id']) for r in csv.DictReader(handle)], list(range(1,41)))
        source = self.work/'v2'
        manifest(source, 'upbit', 4, schema_version=2)
        for number in (1,3):
            (source/'raw'/f'{number}.jsonl.gz').write_bytes(gzip.compress(jsonl([trade('upbit',number)])))
        (source/'raw/2.jsonl.gz').write_bytes(b'broken')
        with self.assertLogs('market-data', level='WARNING'):
            report = norm.replay(norm.mapper_for('upbit'), source, self.work/'v2out')
        self.assertEqual(report['raw_records'],2)
        gaps = [(w['first_seq'],w['last_seq']) for w in report['recovery_warnings'] if w['reason']=='sequence gap']
        self.assertEqual(gaps, [(2,2),(4,4)])

    def test_many_dates_and_cross_run_dedup(self):
        # 淘汰表頭快取後重訪舊日，逐檔計數與首筆成交仍能跨 run 接續。
        source = self.work/'runs'
        for run, started, data in [('a',1,range(1,141)),('b',2,(1,141))]:
            root = source/run
            manifest(root, 'upbit', len(data), started_us=started)
            rows = []
            for seq, tid in enumerate(data,1):
                row = trade('upbit',seq,tid)
                native = json.loads(row['raw'])
                day = 1 if tid==141 else tid
                native['trade_timestamp'] = MS + day*86400000
                rows.append(dict(row, raw=json.dumps(native), local_timestamp=native['trade_timestamp']*1000))
            (root/'raw/1.jsonl').write_bytes(jsonl(rows))
        report = norm.replay(norm.mapper_for('upbit'), source, self.work/'out', batch_size=3)
        self.assertEqual(len(report['rows']),140)
        self.assertEqual(sum(report['rows'].values()),141)
        self.assertEqual(report['mapper_issues']['duplicate_trades'],1)
        key = next(k for k,n in report['rows'].items() if n==2)
        self.assertEqual(len((self.work/'out'/key).read_text().splitlines()),3)

    def test_native_missing_members_and_empty_raw(self):
        # 批次裡每筆損壞成員記錄缺欄位置，仍保留有效成交；全缺時報告完整區間。
        root = self.work/'partial'
        manifest(root, 'bybit', 1)
        member = json.loads(trade('bybit',1)['raw'])['data'][0]
        missing = {k:v for k,v in member.items() if k!='p'}
        (root/'raw/1.jsonl').write_bytes(jsonl([bybit(1,'publicTrade',[member,missing,{}])]))
        with self.assertLogs('market-data', level='WARNING'):
            report = norm.replay(norm.mapper_for('bybit'),root,self.work/'out')
        self.assertEqual(sum(report['rows'].values()),1)
        self.assertEqual([w['item_index'] for w in report['recovery_warnings']], [1,2])
        self.assertIn('p',report['recovery_warnings'][0]['detail'])
        empty = self.work/'missing'
        manifest(empty,'upbit',700)
        with self.assertLogs('market-data', level='WARNING'):
            report = norm.replay(norm.mapper_for('upbit'),empty,self.work/'no-raw')
        self.assertEqual(report['raw_records'], 0)
        self.assertEqual(report['status'],'completed_with_warnings')
        gap = report['recovery_warnings'][0]
        self.assertEqual((gap['first_seq'],gap['last_seq']), (1,700))

    def test_disk_depth_ring_order_overflow_and_reset(self):
        # 環形歷史以提交次序覆蓋、候選按原生 u 排序；不足覆蓋時繼續回報超限。
        module = importlib.import_module('02_normalize_tardis.mappers.binance')
        with closing(sqlite3.connect(':memory:')) as db:
            mapper = module.Mapper(db)
            engine = module.BookEngine('binance-futures',limit=3)
            record = dict(exchange='binance-futures',symbol='BTCUSDT',kind='ws',channel='depth@0ms')
            for update, received in ((20,1),(23,4),(21,3),(22,2)):
                data = dict(u=update)
                current = dict(record,local_timestamp=received,raw=json.dumps(dict(data=data)))
                mapper.merged_depth(current,data,engine)
            self.assertEqual(db.execute('SELECT count(*) FROM depth_history').fetchone()[0],3)
            snap = dict(record,channel='depthSnapshot',request_started_us=4,generation=1)
            mapper.merged_depth(snap,{},engine)
            self.assertEqual([r[0]['u'] for r in engine.pending.buffer],[23])
            self.assertEqual([r[0]['u'] for r in mapper.snapshot_updates(('binance-futures','BTCUSDT'),2)],[21,22,23])
            mapper.merged_depth(dict(snap,request_started_us=1),{},engine)
            self.assertIsNone(engine.pending)
            self.assertEqual(mapper.issues['merged_snapshot_history_overflow'],1)
            mapper.reset()
            self.assertEqual(db.execute('SELECT count(*) FROM depth_history').fetchone()[0],0)

    def test_independent_paths_and_worker_failure(self):
        # 其中一家配置錯誤時，另一家仍完成；多路徑及非零結束碼可供外部排程使用。
        good, missing = self.work/'source-A', self.work/'absent'
        manifest(good,'upbit',1)
        (good/'raw/1.jsonl').write_bytes(jsonl([trade('upbit',1)]))
        a, b = self.work/'export-A', self.work/'export-B'
        cmd = [sys.executable,str(STAGE/'02_normalize_tardis.py'),'--jobs','1',
               '--exchange','bybit','--input',str(missing),'--output',str(a),
               '--exchange','upbit','--input',str(good),'--output',str(b)]
        result = subprocess.run(cmd,capture_output=True,text=True)
        self.assertNotEqual(result.returncode,0)
        data = json.loads(result.stdout)
        self.assertEqual(data['bybit']['status'],'failed')
        self.assertEqual(data['upbit']['status'],'complete')
        self.assertEqual(data['upbit']['csv_rows'],1)
