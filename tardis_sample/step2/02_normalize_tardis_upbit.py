#!/usr/bin/env python3
"""將 Upbit 成交及逐筆完整簿重播為 Tardis CSV。"""
from functools import partial
from common import dec
from market_mapping import NativeMapper, OrderBook, milliseconds
from normalization import cli, replay


def upbit_time(value):
    # 對齊 handy.ts 的門檻，歷史毫秒及近期微秒可出現在同一原生欄位。
    value = int(value)
    return value if value > 10**15 else milliseconds(value)


class Mapper(NativeMapper):
    family = 'upbit'
    types = {'upbit': ('trades', 'quotes')}

    def map_message(self, key, record, obj):
        # trade_timestamp 是成交時間，訂單簿則使用 timestamp。
        if key[2] == 'trade':
            side = 'sell' if obj['ask_bid'] == 'ASK' else 'buy'
            yield 'trades', self.trade(key, obj['sequential_id'], obj['trade_price'], obj['trade_volume'],
                                       side, upbit_time(obj['trade_timestamp']))
        elif key[2] == 'orderbook':
            units = obj['orderbook_units']
            bids = [(u['bid_price'], u['bid_size']) for u in units if dec(u['bid_price']) > 0]
            asks = [(u['ask_price'], u['ask_size']) for u in units if dec(u['ask_price']) > 0]
            state = self.channels.setdefault(key, {})
            timestamp = upbit_time(obj['timestamp'])
            if timestamp < state.get('timestamp', timestamp):
                return
            state['timestamp'] = timestamp
            # 每份 orderbook 都是快照，上一份沒出現的價位不能沿用。
            row = OrderBook().apply(True, bids, asks, timestamp)
            yield 'quotes', self.changed_quote(state, row)


normalize = partial(replay, Mapper)


if __name__ == '__main__':
    cli(Mapper)
