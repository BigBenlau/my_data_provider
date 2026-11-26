#!/usr/bin/env python3
"""第 02 階段，重播 Binance 現貨及 USD-M 期貨並輸出 Tardis CSV。"""
import base64
from collections import Counter
from decimal import Decimal
import json
from common import BBO, TYPES, dec, micros
from common.protocol import validate
from ..normalization import BookError, Side, invalid_payload

class Book:
    """單一 snapshot 世代的 Binance L2 狀態，包含快照前增量緩存及序號接合。"""
    def __init__(self,market,limit=20000):
        # 初始化兩邊訂單簿、緩存上限、最後更新序號與同步狀態。
        self.market=market; self.limit=limit; self.buffer=[]; self.bids=Side(True); self.asks=Side(False)
        self.last=None; self.synced=False; self.bounds=None
    def snapshot(self,d,local,unit):
        # 套用 REST 快照後重播已緩存 delta；記住快照邊界，避免把未知深度當成完整市場。
        self.last=int(d['lastUpdateId']); self.bids.apply(d['bids']); self.asks.apply(d['asks'])
        if not self.bids.values or not self.asks.values: raise BookError('empty_snapshot')
        self.bounds=(min(self.bids.values),max(self.asks.values))
        buffered,self.buffer=self.buffer,[]
        for update,received,u in buffered: self.update(update,received,u)
        if self.synced: return self.quote(micros(d['T'],unit) if 'T' in d else local,local)
    def update(self,d,local,unit):
        # 按 spot 的 U/u 或 futures 的 pu/u 驗證連續性，再套用 delta。
        if self.last is None:
            self.buffer.append((d,local,unit))
            if len(self.buffer)>self.limit: raise BookError('buffer_overflow')
            return None
        u,U=int(d['u']),int(d['U'])
        if self.market=='binance':
            # 現貨丟棄已套用的增量，新批次必須涵蓋下一個更新序號。
            if u<=self.last: return None
            if U>self.last+1: raise BookError('sequence_gap')
        else:
            if u<self.last or (self.synced and u==self.last): return None
            if not self.synced:
                # 期貨首筆先接合快照；之後以 pu 銜接上一筆 u。
                if not (U<=self.last<=u or int(d['pu'])==self.last): raise BookError('snapshot_overlap')
            elif int(d['pu'])!=self.last: raise BookError('sequence_gap')
        self.bids.apply(d['b']); self.asks.apply(d['a']); self.last=u; self.synced=True
        return self.quote(micros(d['E'],unit),local)
    def quote(self,t,local):
        # 僅從已知有效買賣盤產生 BBO；交叉盤或價格越界時報錯。
        bid,bq=self.bids.best(); ask,aq=self.asks.best()
        if bid>=ask: raise BookError('crossed_book')
        if bid<self.bounds[0] or ask>self.bounds[1]: raise BookError('snapshot_coverage')
        return dict(zip(BBO,(aq,ask,bid,bq)),timestamp=t,local_timestamp=local)


class BookEngine:
    """離線重播同一個 WS session 的目前與候選快照，完成接合才換代。"""
    def __init__(self,market,limit=20000):
        # 建立目前／候選 book 容器與最近一次序號問題紀錄。
        self.market=market; self.limit=limit; self.current=None; self.pending=None; self.generation=None
        self.issue=None
    def begin(self,generation):
        # 啟動指定 generation 的候選快照，保留仍有效的目前 book。
        self.pending=Book(self.market,self.limit); self.generation=generation
    def promote(self,row):
        # 候選 book 同步成功後才取代目前 book，避免快照更新造成假空窗。
        if self.pending and self.pending.synced:
            self.current,self.pending=self.pending,None
            return row
    def snapshot(self,d,local,unit,generation):
        # 只接受相符 generation 的快照；候選失敗不破壞目前有效 book。
        if self.pending is None or generation!=self.generation: return None
        try: return self.promote(self.pending.snapshot(d,local,unit))
        except BookError:
            self.pending=None; raise
    def update(self,d,local,unit):
        # 同時更新目前與候選 book；記錄無效原因，必要時停止可信 BBO 輸出。
        self.issue=None; row=None; promoted=None
        if self.current:
            try: row=self.current.update(d,local,unit)
            except BookError as e: self.issue=str(e); self.current=None
        if self.pending:
            try: promoted=self.promote(self.pending.update(d,local,unit))
            except BookError as e: self.issue=str(e); self.pending=None
        return promoted or row

class Mapper:
    """依固定 Tardis 規則維護每個市場／商品的 book、ticker 與交易去重狀態。"""
    family = 'binance'
    types = TYPES
    limitations = ()
    def __init__(self,db):
        # 建立磁碟 SQLite trade ID 索引與可重置的行情狀態，控制長期重播記憶體用量。
        self.db=db;db.execute('CREATE TABLE IF NOT EXISTS seen (exchange TEXT,symbol TEXT,id TEXT,PRIMARY KEY(exchange,symbol,id)) WITHOUT ROWID')
        self.books={};self.tickers={};self.sessions={};self.previous_bbo={};self.issues=Counter()
        self.depth_history={}
        db.execute('''CREATE TABLE IF NOT EXISTS depth_history (market, symbol, slot INTEGER, serial INTEGER, received INTEGER,
                   unit TEXT, update_id TEXT, raw, encoded INTEGER, wrapped INTEGER,
                   PRIMARY KEY(market,symbol,slot)) WITHOUT ROWID''')
        db.execute('DELETE FROM depth_history')
    def reset(self):
        # 跨 run 重置 book／ticker/session，但保留跨 run 的交易 ID 去重索引。
        self.books.clear();self.tickers.clear();self.sessions.clear();self.previous_bbo.clear()
        if self.depth_history:
            self.db.execute('DELETE FROM depth_history')
            self.depth_history.clear()
    def depth_engine(self, record):
        # 每次 session 變更都捨棄舊訂單簿；記憶體只保留每個商品的目前狀態。
        if not record['channel'].startswith('depth'):
            return None
        pair = record['exchange'], record['symbol']
        if self.sessions.get(pair) != record['session']:
            self.books[pair] = BookEngine(pair[0])
            self.sessions[pair] = record['session']
            self.previous_bbo.pop(pair, None)
            if self.depth_history.pop(pair, None) is not None:
                self.db.execute('DELETE FROM depth_history WHERE market=? AND symbol=?', pair)
            if record.get('legacy_schema'):
                self.books[pair].begin(-1)
        return self.books[pair]

    def lifecycle(self, record, engine):
        # 控制事件只改變可信狀態；斷線或快照缺口期間不產生合併後的行情列。
        pair = record['exchange'], record['symbol']
        event, channel = record['event'], record['channel']
        if engine is not None:
            if event == 'snapshot_begin':
                engine.begin(record['generation'])
            elif event in ('session_end', 'disconnected', 'book_gap'):
                self.books[pair] = BookEngine(pair[0])
                self.previous_bbo.pop(pair, None)
                if record.get('legacy_schema'):
                    self.books[pair].begin(-1)
            elif event == 'snapshot_error':
                engine.pending = None
        if channel in ('ticker', 'markPrice@1s', 'openInterest'):
            if event in ('session_end', 'disconnected', 'rest_error', 'rest_failure'):
                self.tickers.pop(pair, None)

    def trade(self, record, data):
        # 僅在第 02 階段過濾指定成交類別，並以市場、商品及成交 ID 去重。
        if data.get('X') in ('INSURANCE_FUND', 'ADL', 'NA'):
            self.issues['off_book_trades'] += 1
            return None
        tid = str(data['t'])
        identity = record['exchange'], record['symbol'], tid
        if not self.db.execute('INSERT OR IGNORE INTO seen VALUES (?,?,?)', identity).rowcount:
            self.issues['duplicate_trades'] += 1
            return None
        return dict(id=tid, side='sell' if data['m'] else 'buy', price=dec(data['p']),
                    amount=dec(data['q']), timestamp=micros(data['T'], record.get('time_unit', 'ms')))

    def quote(self, record, data, engine):
        # 對應上游深度一檔、間隔零的輸出條件；本地另保留嚴格接合檢查。
        local, unit = record['local_timestamp'], record.get('time_unit', 'ms')
        if record.get('merged'):
            self.merged_depth(record, data, engine)
        if record['channel'] == 'depthSnapshot':
            row = engine.snapshot(data, local, unit, record['generation'])
        else:
            row = engine.update(data, local, unit)
            if engine.issue:
                self.issues[engine.issue] += 1
        if row:
            pair = record['exchange'], record['symbol']
            state = tuple(row[k] for k in BBO)
            if state == self.previous_bbo.get(pair):
                return None
            self.previous_bbo[pair] = state
        return row

    def merged_depth(self, record, data, engine):
        # 以磁碟環形索引保留原文，避免每商品在記憶體常駐兩萬份已解碼深度。
        pair = record['exchange'], record['symbol']
        serial = self.depth_history.get(pair, 0)
        if record['channel'] != 'depthSnapshot':
            self.db.execute('INSERT OR REPLACE INTO depth_history VALUES (?,?,?,?,?,?,?,?,?,?)',
                            (*pair, serial % engine.limit, serial, record['local_timestamp'], record.get('time_unit', 'ms'),
                             str(data['u']), record['raw'], record.get('encoding') == 'base64', record['kind'] == 'ws'))
            self.depth_history[pair] = serial + 1
            return
        started = record['request_started_us']
        engine.begin(record['generation'])
        if serial >= engine.limit:
            oldest = self.db.execute('SELECT received FROM depth_history WHERE market=? AND symbol=? AND slot=?',
                                     (*pair, serial % engine.limit)).fetchone()[0]
            if oldest > started:
                engine.pending = None
                self.issues['merged_snapshot_history_overflow'] += 1
                return
        engine.pending.buffer = self.snapshot_updates(pair, started)

    def snapshot_updates(self, pair, started):
        # 只排序符合請求時間的輕量序號；同 u 沿用提交順序，一次還原一筆原文。
        entries = self.db.execute('SELECT update_id,serial,slot FROM depth_history '
                                  'WHERE market=? AND symbol=? AND received>=?', (*pair, started))
        order = sorted((int(update), serial, slot) for update, serial, slot in entries)
        for _, _, slot in order:
            raw, encoded, wrapped, local, unit = self.db.execute(
                'SELECT raw,encoded,wrapped,received,unit FROM depth_history WHERE market=? AND symbol=? AND slot=?',
                (*pair, slot)).fetchone()
            payload = json.loads(base64.b64decode(raw) if encoded else raw, parse_float=Decimal)
            yield payload['data'] if wrapped else payload, local, unit

    def ticker(self, record, data):
        # 合併衍生品欄位，未知值保持空白；OI 不可直接替換行情事件時間。
        pair = record['exchange'], record['symbol']
        channel, unit = record['channel'], record.get('time_unit', 'ms')
        state = self.tickers.setdefault(pair, {})
        updates = {}
        if channel == 'ticker':
            updates['last_price'] = dec(data['c'])
        elif channel == 'openInterest':
            updates['open_interest'] = dec(data['openInterest'])
        else:
            updates['mark_price'] = dec(data['p'])
            if 'i' in data:
                updates['index_price'] = dec(data['i'])
            if data.get('r', '') != '' and data.get('T', 0) != 0:
                updates.update(funding_rate=dec(data['r']), funding_timestamp=micros(data['T'], 'ms'))
        if channel != 'openInterest':
            state['timestamp'] = max(state.get('timestamp', 0), micros(data['E'], unit))
        changed = any(state.get(k) != value for k, value in updates.items())
        state.update(updates)
        if changed:
            state.setdefault('timestamp', record['local_timestamp'])
            return dict(state)
        return None

    def map_payload(self, record, data, engine):
        # 按原生 channel 分派固定映射規則；價格與數量始終使用 Decimal。
        channel, unit = record['channel'], record.get('time_unit', 'ms')
        if channel == 'trade':
            return 'trades', self.trade(record, data)
        if channel == 'bookTicker':
            timestamp = micros(data['E'], unit) if 'E' in data else record['local_timestamp']
            return 'book_ticker', dict(zip(BBO, [dec(data[k]) for k in ('A', 'a', 'b', 'B')]), timestamp=timestamp)
        if engine is not None:
            return 'quotes', self.quote(record, data, engine)
        if channel == 'forceOrder':
            order = data['o']
            if order['X'] == 'FILLED':
                # 使用委託價格 p 與累計成交量 z，不能替換為平均成交價或原始委託量。
                return 'liquidations', dict(id=None, side=order['S'].lower(), price=dec(order['p']),
                                            amount=dec(order['z']), timestamp=micros(order['T'], unit))
        if channel in ('ticker', 'markPrice@1s', 'openInterest'):
            return 'derivative_ticker', self.ticker(record, data)
        return None, None

    def consume(self, record):
        # 按原始接收順序重播，將協議解碼、生命週期與各類映射分開處理。
        if record.get('legacy_schema') and record['kind'] in ('message', 'data'):
            kind = 'rest' if record['channel'] in ('depthSnapshot', 'openInterest') else 'ws'
            record = dict(record, kind=kind, generation=-1)
        market, symbol, channel = record['exchange'], record['symbol'], record['channel']
        if market not in TYPES or not symbol:
            return []
        engine = self.depth_engine(record)
        if record['kind'] == 'event':
            self.lifecycle(record, engine)
            return []
        if record['kind'] not in ('ws', 'rest'):
            return []
        try:
            raw = base64.b64decode(record['raw']) if record.get('encoding') == 'base64' else record['raw']
            data = json.loads(raw, parse_float=Decimal)
            if record['kind'] == 'ws':
                if not isinstance(data, dict) or data.get('stream') != f'{symbol.lower()}@{channel}':
                    return []
                data = data['data']
            validate(data, channel, symbol)
            target, row = self.map_payload(record, data, engine)
            if row is not None:
                return [(target, dict(row, exchange=market, symbol=symbol,
                                      local_timestamp=row.get('local_timestamp', record['local_timestamp'])))]
        except BookError as exc:
            self.issues[str(exc)] += 1
        except (ValueError, TypeError, KeyError) as exc:
            invalid_payload(self, record, exc)
        return []
