#!/usr/bin/env python3
"""將 Coinbase Exchange 的 matches、level2_batch 與 ticker 轉為 Tardis CSV。"""
from collections import OrderedDict
from datetime import datetime, timezone
import re
from common import dec
from ..market_mapping import NativeMapper, OrderBook, SPOT, best_row

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def iso_time(value):
    # 保留六位微秒，以整數時間差換算 epoch，並相容 Python 3.10。
    if value is None:
        return None
    value = re.sub(r'\.(\d+)', lambda m: '.' + m[1][:6].ljust(6, '0'), value)
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('timestamp without timezone')
    delta = stamp - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


class Mapper(NativeMapper):
    family = 'coinbase'
    types = {'coinbase': SPOT}
    limitations = ('level2_batch 無連續序號，按來源 session 重建，無法證明兩次報價之間沒有漏訊。',)

    def map_message(self, key, record, obj):
        # last_match 是訂閱初始通知，上游只把 match 當作新成交。
        kind = obj['type']
        if kind == 'last_match':
            return
        ts = iso_time(obj.get('time'))
        if kind == 'match':
            if ts is None:
                raise ValueError('missing trade timestamp')
            side = 'buy' if obj['side'] == 'sell' else 'sell'
            yield 'trades', self.trade(key, obj['trade_id'], obj['price'], obj['size'], side, ts)
        elif kind == 'ticker':
            fields = ('best_bid', 'best_bid_size', 'best_ask', 'best_ask_size')
            bid, bq, ask, aq = (dec(obj[k]) if k in obj else None for k in fields)
            timestamp = record['local_timestamp'] if ts is None or ts < 0 else ts
            yield 'book_ticker', best_row(timestamp, (bid, bq), (ask, aq))
        elif kind in ('snapshot', 'l2update'):
            yield 'quotes', self.depth(key, record, obj, ts)

    def depth(self, key, record, obj, ts):
        # 各條 level2_batch 可有不同批次邊界，各自從快照建簿，不能混套兩路增量。
        state = self.channels.setdefault(key, {})
        sessions = state.setdefault('sources', OrderedDict())
        source = record.get('source_session') or record.get('session')
        snapshot = obj['type'] == 'snapshot'
        if source not in sessions:
            if not snapshot:
                self.issues['depth_before_snapshot'] += 1
                return None
            if len(sessions) >= 3:
                sessions.popitem(last=False)
            sessions[source] = OrderBook()
        book = sessions[source]
        sessions.move_to_end(source)
        if snapshot:
            bid, ask = self.snapshot_levels(obj['bids']), self.snapshot_levels(obj['asks'])
            ts = record['local_timestamp'] if ts is None or ts < 0 else ts
            book.last_update_time = None
        else:
            bid = [(p, q) for side, p, q in obj['changes'] if side == 'buy']
            ask = [(p, q) for side, p, q in obj['changes'] if side == 'sell']
            if ts is None or ts < 0:
                ts = book.last_update_time
                if ts is None:
                    return None
            if book.last_update_time is not None and ts < book.last_update_time:
                return None
            book.last_update_time = ts
        row = book.apply(snapshot, bid, ask, ts)
        # 跨路只發布時間往前的簿；同時間保留已選來源自身的連續更新。
        if ts < state.get('timestamp', ts) or (ts == state.get('timestamp') and source != state['source']):
            return None
        state.update(timestamp=ts, source=source)
        return self.changed_quote(state, row)

    @staticmethod
    def snapshot_levels(levels):
        # 上游略過快照內負數或非數值的數量。
        result = []
        for price, size in levels:
            try:
                amount = dec(size)
            except ValueError:
                continue
            if amount >= 0:
                result.append((price, amount))
        return result
