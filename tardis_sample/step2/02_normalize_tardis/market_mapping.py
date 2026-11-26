"""非 Binance 行情共用的解碼、成交索引、訂單簿與衍生品狀態。"""
from collections import Counter
from common import BBO, dec, encode
from common.protocol import classify, decode
from .normalization import Side, invalid_payload

SPOT = ('trades', 'quotes', 'book_ticker')
RESET = ('disconnected', 'session_end', 'book_gap')


def milliseconds(value):
    # 交易所發布的毫秒轉為整數微秒。
    return int(value) * 1000


def best_row(timestamp, bid, ask):
    # 空邊保留欄位空值，價量順序依 Tardis 的 CSV 表頭。
    return dict(zip(BBO, (ask[1], ask[0], bid[0], bid[1])), timestamp=timestamp)


class OrderBook:
    def __init__(self):
        # 第一份有效快照抵達後才能接受增量。
        self.ready = False
        self.sequence = None
        self.timestamp = None
        self.bids, self.asks = Side(True), Side(False)

    def apply(self, snapshot, bids, asks, timestamp, sequence=None):
        # 完整驗證本筆價位後才修改訂單簿，避免只套用半筆更新。
        if not snapshot and not self.ready:
            return None
        bids, asks = self.levels(bids), self.levels(asks)
        if snapshot:
            self.bids, self.asks = Side(True), Side(False)
        self.bids.apply(bids)
        self.asks.apply(asks)
        self.ready, self.sequence, self.timestamp = True, sequence, timestamp
        bid = self.bids.best() if self.bids.values else (None, None)
        ask = self.asks.best() if self.asks.values else (None, None)
        return best_row(timestamp, bid, ask)

    @staticmethod
    def levels(values):
        # 各所價位可能帶有額外欄位，前兩個是價格與絕對數量。
        result = []
        for level in values:
            price, amount = dec(level[0]), dec(level[1])
            if price <= 0 or amount < 0:
                raise ValueError('invalid book level')
            result.append((price, amount))
        return result


class NativeMapper:
    limitations = ()
    ticker_channels = ()

    def __init__(self, db):
        # 成交身分使用磁碟索引，記憶體只維護當前批次的狀態。
        self.db = db
        db.execute('CREATE TABLE seen_trade (market, symbol, id, time, side, price, amount, '
                   'PRIMARY KEY (market, symbol, id, time, side, price, amount)) WITHOUT ROWID')
        self.issues = Counter()
        self.reset()

    def reset(self):
        # 重置連線、訂單簿及 ticker，跨批次仍可辨識已輸出成交。
        self.sessions = {}
        self.channels = {}
        self.tickers = {}

    def invalidate(self, key):
        # 只清除此來源影響的狀態，多來源衍生品合併則重新暖機。
        self.channels.pop(key, None)
        if key[2] in self.ticker_channels:
            self.tickers.pop(key[:2], None)

    def consume(self, record):
        # 核對原生商品與 channel，略過 ACK、心跳及重連期間的非行情資料。
        market, symbol, channel = record['exchange'], record['symbol'], record['channel']
        if not symbol or market not in self.types:
            return []
        key = market, symbol, channel
        session = record.get('session')
        if key in self.sessions and self.sessions[key] != session:
            self.invalidate(key)
        self.sessions[key] = session
        if record['kind'] == 'event':
            if record.get('event') in RESET:
                self.invalidate(key)
            return []
        if record['kind'] not in ('ws', 'message', 'data') or record.get('raw') == 'pong':
            return []
        try:
            obj = decode(record)
            kind, _ = classify(record, obj)
            if kind != 'data':
                return []
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            self.invalidate(key)
            invalid_payload(self, record, exc)
            return []
        return self.map_items(key, record, obj)

    def map_items(self, key, record, obj):
        # 未拆分的舊版批次逐項處理，單項損壞仍可保留其餘可驗證行情。
        items = obj.get('data')
        messages = (dict(obj, data=[item]) for item in items) if isinstance(items, list) else (obj,)
        rows = []
        for index, message in enumerate(messages):
            try:
                for target, row in self.map_message(key, record, message):
                    if row is not None:
                        rows.append((target, dict(row, exchange=key[0], symbol=key[1],
                                                  local_timestamp=record['local_timestamp'])))
            except (ValueError, TypeError, KeyError, IndexError, AttributeError) as exc:
                self.invalidate(key)
                invalid_payload(self, record, exc, index)
        return rows

    def trade(self, key, tid, price, amount, side, timestamp):
        # 以正規化後仍可區分的事件欄位去除相同成交，保留同 ID 的內容差異。
        price, amount = dec(price), dec(amount)
        identity = (*key[:2], str(tid))
        seen = self.db.execute('SELECT 1 FROM seen_trade WHERE market=? AND symbol=? AND id=? LIMIT 1', identity).fetchone()
        row = (*identity, timestamp, side, encode(price), encode(amount))
        if not self.db.execute('INSERT OR IGNORE INTO seen_trade VALUES (?,?,?,?,?,?,?)', row).rowcount:
            self.issues['duplicate_trades'] += 1
            return None
        if seen:
            self.issues['trade_id_conflict'] += 1
        return dict(id=str(tid), price=price, amount=amount, side=side, timestamp=timestamp)

    def changed_quote(self, state, row):
        # 最佳買賣價或數量改變時才寫 quotes，其他檔位仍保留在簿內。
        if row is None:
            return None
        best = tuple(row[field] for field in BBO)
        if best == state.get('best', (None,) * 4):
            return None
        state['best'] = best
        return row

    def ticker(self, key, timestamp, updates):
        # 時間單調遞增，只有行情欄位變動才輸出累積狀態。
        state = self.tickers.setdefault(key[:2], {})
        state['timestamp'] = max(timestamp, state.get('timestamp', timestamp))
        changed = any(state.get(k) != v for k, v in updates.items())
        state.update(updates)
        return dict(state) if changed else None
