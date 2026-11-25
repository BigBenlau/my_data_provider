#!/usr/bin/env python3
"""將 OKX V5 的現有公開來源轉成 okex 與 okex-swap 的 Tardis CSV。"""
from common import dec
from ..market_mapping import NativeMapper, OrderBook, SPOT, best_row, milliseconds


class Mapper(NativeMapper):
    family = 'okx'
    types = {'okex': SPOT, 'okex-swap': (*SPOT, 'derivative_ticker')}
    ticker_channels = ('tickers', 'mark-price', 'open-interest', 'funding-rate')
    limitations = ('OKX trades 來源為聚合成交，與 trades-all 的逐筆資料粒度不同。',
                   '現有永續採集缺少 index-tickers 及清算來源，index_price 留空，無清算 CSV。')

    def map_message(self, key, record, obj):
        # OKX 每個陣列元素都有自己的事件時間，合約成交量保留原生張數。
        market, symbol, channel = key
        for item in obj['data']:
            if item.get('instId', symbol) != symbol:
                raise ValueError('unexpected instrument')
            if channel == 'trades':
                side = 'buy' if item['side'] == 'buy' else 'sell'
                yield 'trades', self.trade(key, item['tradeId'], item['px'], item['sz'], side, milliseconds(item['ts']))
            elif channel == 'bbo-tbt':
                bid = self.first_level(item.get('bids', []))
                ask = self.first_level(item.get('asks', []))
                yield 'book_ticker', best_row(milliseconds(item['ts']), bid, ask)
            elif channel == 'books':
                yield 'quotes', self.depth(key, obj['action'], item)
            elif market == 'okex-swap' and channel in self.ticker_channels:
                yield 'derivative_ticker', self.derivative(key, item, record['local_timestamp'])

    @staticmethod
    def first_level(levels):
        # bbo-tbt 可只帶一側，缺側輸出空值。
        return (dec(levels[0][0]), dec(levels[0][1])) if levels else (None, None)

    def depth(self, key, action, item):
        # 用 prevSeqId 接合，原生序號可在維護時倒退，接得上前序仍是有效更新。
        if action not in ('snapshot', 'update'):
            raise ValueError('invalid book action')
        snapshot = action == 'snapshot'
        ts = milliseconds(item['ts'])
        if not ts:
            return None
        state = self.channels.setdefault(key, {})
        if 'book' not in state:
            state['book'] = OrderBook()
        book = state['book']
        sequence = int(item['seqId'])
        if not snapshot and not book.ready:
            self.issues['depth_before_snapshot'] += 1
            return None
        if snapshot and (ts < state.get('gap_timestamp', ts) or (book.ready and ts < book.timestamp)):
            return None
        if not snapshot:
            previous = int(item['prevSeqId'])
            if sequence == book.sequence and (previous != sequence or not (item['bids'] or item['asks'])):
                return None
            if previous != book.sequence:
                if ts < book.timestamp or (ts == book.timestamp and previous < sequence < book.sequence):
                    return None
                self.issues['sequence_gap'] += 1
                # 遲到的舊快照不能填補發生在它之後的斷層。
                self.channels[key] = {'gap_timestamp': ts}
                return None
        row = book.apply(snapshot, item['bids'], item['asks'], ts, sequence)
        return self.changed_quote(state, row)

    def derivative(self, key, item, local):
        # 永續標記價、未平倉量及最新價只採正值，資金費率零值有效。
        channel = key[2]
        if channel == 'funding-rate':
            updates = {}
            for field, target in (('fundingRate', 'funding_rate'), ('nextFundingRate', 'predicted_funding_rate')):
                if item.get(field) not in (None, ''):
                    updates[target] = dec(item[field])
            if item.get('fundingTime') not in (None, ''):
                updates['funding_timestamp'] = milliseconds(item['fundingTime'])
        else:
            source, target = {'tickers': ('last', 'last_price'), 'open-interest': ('oi', 'open_interest'),
                              'mark-price': ('markPx', 'mark_price')}[channel]
            if item.get(source) in (None, '') or dec(item[source]) <= 0:
                return None
            updates = {target: dec(item[source])}
        previous = self.tickers.get(key[:2], {}).get('timestamp', local)
        ts = milliseconds(item['ts']) if item.get('ts') not in (None, '') else previous
        return self.ticker(key, ts, updates)
