"""各交易所原生訊息的固定回歸案例與第 02 階段離線重播。"""
import base64
import csv
import importlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import SCHEMAS

engine = importlib.import_module('02_normalize_tardis.normalization')
mapper_for, replay = engine.mapper_for, engine.replay

MS = 1795610400000
LOCAL = MS * 1000 + 987


def record(market, symbol, channel, obj, **extra):
    # 建立與 supervisor JSONL 一致的封裝，測試保留來源時間和接收時間。
    return dict(kind='ws', exchange=market, symbol=symbol, channel=channel, session='merged',
                local_timestamp=LOCAL, raw=json.dumps(obj), **extra)


def bybit(channel, data, kind='snapshot', ts=MS, market='bybit'):
    # V5 公開來源用 topic 指出 channel 和商品。
    return record(market, 'BTCUSDT', channel, dict(topic=channel + '.BTCUSDT', type=kind, ts=ts, data=data))


def okx(channel, data, action=None):
    # V5 不同頻道共用商品與批次封裝。
    obj = dict(arg=dict(channel=channel, instId='BTC-USDT-SWAP'), data=[data])
    if action:
        obj['action'] = action
    return record('okex-swap', 'BTC-USDT-SWAP', channel, obj)


def coin(channel, kind, source='A', **fields):
    # 同一商品的兩條實體連線共用邏輯 session，各自保存來源身分。
    obj = dict(type=kind, product_id='BTC-USD', **fields)
    return record('coinbase', 'BTC-USD', channel, obj, source_session=source)


class MarketMappingTests(unittest.TestCase):
    def mapper(self, family):
        # 每個測試使用獨立 SQLite，避免跨案例去重。
        db = sqlite3.connect(':memory:')
        self.addCleanup(db.close)
        return mapper_for(family)(db)

    def test_bybit_trade_batch_and_liquidation(self):
        # 檢查拆批、壞元素、跨副本重送、同 ID 衝突與清算的持倉反向。
        mapper = self.mapper('bybit')
        item = dict(s='BTCUSDT', i='9007199254740999', p='123.100000000000000001', v='2', S='Buy', T=MS)
        other = dict(item, i='2', S='Sell')
        rows = mapper.consume(bybit('publicTrade', [item, {}, other]))
        self.assertEqual([r['side'] for _, r in rows], ['buy', 'sell'])
        self.assertEqual(str(rows[0][1]['price']), item['p'])
        self.assertEqual(rows[0][1]['timestamp'], MS * 1000)
        self.assertEqual(mapper.consume(bybit('publicTrade', [item])), [])
        self.assertEqual(len(mapper.consume(bybit('publicTrade', [dict(item, v='3')]))), 1)
        self.assertEqual(mapper.issues['trade_id_conflict'], 1)
        target, row = mapper.consume(bybit('allLiquidation', [item]))[0]
        self.assertEqual((target, row['side'], row['id'], row['amount']), ('liquidations', 'sell', None, 2))

    def test_bybit_depth_and_native_best(self):
        # 深度五十檔影響 quotes，一檔原生來源獨立處理缺側、刪檔與服務重置。
        mapper = self.mapper('bybit')
        book = dict(s='BTCUSDT', b=[['10', '1'], ['9', '4']], a=[['12', '1']], u=900)
        self.assertFalse(mapper.consume(bybit('orderbook.50', book, 'delta')))
        self.assertEqual(mapper.consume(bybit('orderbook.50', book))[0][1]['bid_price'], 10)
        delta = dict(book, b=[['10', '0']], a=[], u=950)
        self.assertEqual(mapper.consume(bybit('orderbook.50', delta, 'delta'))[0][1]['bid_price'], 9)
        self.assertFalse(mapper.consume(bybit('orderbook.50', delta, 'delta')))
        self.assertEqual(mapper.consume(bybit('orderbook.50', dict(book, u=1), ts=MS+1))[0][1]['bid_price'], 10)
        self.assertFalse(mapper.consume(bybit('orderbook.1', book, 'delta')))
        self.assertEqual(mapper.consume(bybit('orderbook.1', book))[0][0], 'book_ticker')
        self.assertEqual(mapper.consume(bybit('orderbook.1', delta, 'delta'))[0][1]['bid_price'], 10)
        self.assertIsNone(mapper.consume(bybit('orderbook.1', dict(book, b=[])))[0][1]['bid_price'])

    def test_bybit_ticker_state(self):
        # 數值相同及空字串不觸發輸出，事件時間不得倒退。
        mapper = self.mapper('bybit')
        ticker = dict(symbol='BTCUSDT', lastPrice='10', nextFundingTime=MS+100, fundingRate='0')
        first = mapper.consume(bybit('tickers', ticker))[0][1]
        self.assertEqual((first['funding_rate'], first['funding_timestamp']), (0, (MS+100)*1000))
        self.assertFalse(mapper.consume(bybit('tickers', dict(ticker, lastPrice='10.0'), ts=MS+3)))
        later = mapper.consume(bybit('tickers', dict(symbol='BTCUSDT', lastPrice='', markPrice='11'), ts=MS+1))[0][1]
        self.assertEqual((later['timestamp'], later['last_price'], later['mark_price']), ((MS+3)*1000, 10, 11))
        self.assertFalse(mapper.consume(bybit('tickers', ticker, market='bybit-spot')))

    def test_okx_previous_sequence_and_reset(self):
        # OKX 前序接得上即可接受維護倒序，斷層後須重新取得快照。
        mapper = self.mapper('okx')
        item = dict(bids=[['10', '1', '0', '3']], asks=[['11', '2']], ts=MS, seqId=100, prevSeqId=-1)
        mapper.consume(okx('books', item, 'snapshot'))
        reset = dict(item, seqId=3, prevSeqId=100, bids=[['10', '5']], asks=[], ts=MS+1)
        self.assertEqual(mapper.consume(okx('books', reset, 'update'))[0][1]['bid_amount'], 5)
        self.assertFalse(mapper.consume(okx('books', reset, 'update')))
        self.assertFalse(mapper.consume(okx('books', dict(reset, seqId=5, prevSeqId=4), 'update')))
        self.assertFalse(mapper.consume(okx('books', dict(reset, seqId=6, prevSeqId=5), 'update')))
        self.assertEqual(mapper.issues['sequence_gap'], 1)
        self.assertFalse(mapper.consume(okx('books', item, 'snapshot')))
        self.assertEqual(mapper.consume(okx('books', dict(item, ts=MS+2), 'snapshot'))[0][1]['bid_amount'], 1)
        mapper.consume(dict(okx('books', item), kind='event', event='disconnected'))
        self.assertFalse(mapper.consume(okx('books', reset, 'update')))

    def test_okx_swap_sources_and_amount_units(self):
        # 成交與 OI 取原始張數，費率時間用 fundingTime，缺少的指數價留白。
        mapper = self.mapper('okx')
        trade = dict(tradeId='777', side='sell', px='123', sz='4', count='3', ts=MS)
        row = mapper.consume(okx('trades', trade))[0][1]
        self.assertEqual((row['id'], row['side'], row['amount']), ('777', 'sell', 4))
        row = mapper.consume(okx('bbo-tbt', dict(asks=[], bids=[['120','3','0','2']], ts=MS)))[0][1]
        self.assertEqual((row['ask_price'], row['bid_amount']), (None, 3))
        mapper.consume(okx('open-interest', dict(oi='4', oiCcy='80', ts=MS+1)))
        row = mapper.consume(okx('funding-rate', dict(fundingRate='0', nextFundingRate='-.001',
                                                      fundingTime=MS+100, nextFundingTime=MS+200, ts=MS)))[0][1]
        self.assertEqual((row['open_interest'], row['funding_timestamp'], row['timestamp']), (4,(MS+100)*1000,(MS+1)*1000))
        self.assertEqual(str(row['predicted_funding_rate']), '-0.001')
        self.assertNotIn('index_price', row)
        self.assertFalse(mapper.consume(okx('mark-price', dict(markPx='0', ts=MS))))
        self.assertNotIn('liquidations', mapper.types['okex-swap'])

    def test_coinbase_side_and_microseconds(self):
        # match 的 side 表示掛單方，ISO 時間保留微秒，初始 last_match 不重複計入。
        mapper = self.mapper('coinbase')
        trade = dict(trade_id=2, size='1', price='5', side='sell', time='1970-01-01T00:00:01.123456789Z')
        self.assertFalse(mapper.consume(coin('matches', 'last_match', **trade)))
        row = mapper.consume(coin('matches', 'match', **trade))[0][1]
        self.assertEqual((row['side'], row['timestamp'], row['local_timestamp']), ('buy', 1123456, LOCAL))
        row = mapper.consume(coin('ticker', 'ticker', best_bid='5'))[0][1]
        self.assertEqual((row['bid_price'], row['ask_price'], row['timestamp']), (5, None, LOCAL))

    def test_coinbase_keeps_independent_batch_books(self):
        # 兩路快照不得覆寫另一條路的簿，遲到資料可更新自己的簿但不能回滾已發布報價。
        mapper = self.mapper('coinbase')
        snap = dict(time='1970-01-01T00:00:01Z', bids=[['9','2'],['8','3']], asks=[['11','1']])
        mapper.consume(coin('level2_batch', 'snapshot', **snap))
        self.assertFalse(mapper.consume(coin('level2_batch', 'snapshot', 'B', **dict(snap, bids=[['7','1']]))))
        update = dict(time='1970-01-01T00:00:02Z', changes=[['buy','9','0']])
        self.assertEqual(mapper.consume(coin('level2_batch','l2update', **update))[0][1]['bid_price'], 8)
        old = dict(time='1970-01-01T00:00:01.5Z', changes=[['sell','10','1']])
        self.assertFalse(mapper.consume(coin('level2_batch', 'l2update', 'B', **old)))
        new = dict(time='1970-01-01T00:00:03Z', changes=[['buy','6','1']])
        row = mapper.consume(coin('level2_batch', 'l2update', 'B', **new))[0][1]
        self.assertEqual((row['bid_price'], row['ask_price']), (7, 10))
        invalid_time = dict(time='0001-01-01T00:00:00Z', changes=[['sell','10','2']])
        self.assertEqual(mapper.consume(coin('level2_batch','l2update','B', **invalid_time))[0][1]['timestamp'], 3000000)

    def test_upbit_binary_and_timestamp_units(self):
        # 同一個來源可傳毫秒或微秒，完整簿須刪掉下一份快照未提供的檔位。
        mapper = self.mapper('upbit')
        obj = dict(type='trade', code='KRW-BTC', sequential_id=999, trade_volume=2, trade_price=100,
                   ask_bid='ASK', trade_timestamp=MS * 1000 + 17, timestamp=MS)
        message = record('upbit', 'KRW-BTC', 'trade', obj)
        row = mapper.consume(dict(message, encoding='base64', raw=base64.b64encode(message['raw'].encode()).decode()))[0][1]
        self.assertEqual((row['side'], row['id'], row['timestamp']), ('sell', '999', MS*1000+17))
        level = dict(bid_price=5, bid_size=1, ask_price=8, ask_size=2)
        book = dict(type='orderbook', code='KRW-BTC', timestamp=MS, orderbook_units=[level])
        msg = record('upbit', 'KRW-BTC', 'orderbook', book)
        self.assertEqual(mapper.consume(msg)[0][1]['timestamp'], MS*1000)
        self.assertFalse(mapper.consume(msg))
        book.update(timestamp=MS+1, orderbook_units=[dict(level,bid_price=0)])
        self.assertIsNone(mapper.consume(record('upbit', 'KRW-BTC', 'orderbook', book))[0][1]['bid_price'])

    def test_replay_window_and_market_guard(self):
        # 在窗前暖機再依事件日期輸出，檢查表頭、版本紀錄、覆寫保護與跨市場拒絕。
        module = mapper_for('bybit')
        data = dict(symbol='BTCUSDT', lastPrice='20')
        events = [bybit('tickers', data), bybit('tickers', dict(data, markPrice='19'), ts=MS+1)]
        with tempfile.TemporaryDirectory() as work:
            root = Path(work)
            (root/'raw').mkdir()
            (root/'manifest.json').write_text(json.dumps(dict(config=dict(markets=['bybit']), started_us=LOCAL,
                                                    status='stopped', raw_records=2, schema_version=3)))
            lines = [json.dumps(dict(event, seq=i)) for i, event in enumerate(events, 1)]
            (root/'raw'/'0001.jsonl').write_text('\n'.join(lines)+'\n')
            out = root/'csv'
            report = replay(module, root, out, start=(MS+1)*1000)
            self.assertTrue(report['mapper_reference']['sources']['bybit.ts']['sha256'])
            path = next(out.glob('bybit/derivative_ticker/*.csv'))
            with path.open() as handle:
                reader = csv.DictReader(handle)
                self.assertEqual(reader.fieldnames, SCHEMAS['derivative_ticker'])
                rows = list(reader)
            self.assertEqual([(r['last_price'],r['mark_price'],r['predicted_funding_rate']) for r in rows], [('20','19','')])
            self.assertEqual(len(next(out.glob('bybit/liquidations/*.csv')).read_text().splitlines()), 1)
            with self.assertRaises(FileExistsError):
                replay(module, root, out)
            with self.assertRaisesRegex(ValueError, 'unexpected markets'):
                replay(mapper_for('okx'), root, root/'wrong')
            self.assertFalse((root/'wrong').exists())
