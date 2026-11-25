#!/usr/bin/env python3
"""將 Bybit V5 現貨與線性永續的原始資料轉成 Tardis CSV。"""
from common import dec
from ..market_mapping import NativeMapper, OrderBook, SPOT, best_row, milliseconds


class Mapper(NativeMapper):
    family = 'bybit'
    types = {'bybit-spot': SPOT, 'bybit': (*SPOT, 'derivative_ticker', 'liquidations')}
    ticker_channels = ('tickers',)

    def map_message(self, key, record, obj):
        # V5 批次可來自原生封包或 supervisor 已拆分的單事件封包。
        market, symbol, channel = key
        data = obj['data']
        if channel in ('publicTrade', 'allLiquidation'):
            for item in data:
                if item['s'] != symbol:
                    raise ValueError('unexpected symbol')
                ts = milliseconds(item['T'])
                if channel == 'publicTrade':
                    side = {'Buy': 'buy', 'Sell': 'sell'}.get(item['S'], 'unknown')
                    yield 'trades', self.trade(key, item['i'], item['p'], item['v'], side, ts)
                elif market == 'bybit':
                    yield 'liquidations', dict(id=None, price=dec(item['p']), amount=dec(item['v']),
                                               side='sell' if item['S'] == 'Buy' else 'buy', timestamp=ts)
            return
        if data.get('s', data.get('symbol')) != symbol:
            raise ValueError('unexpected symbol')
        ts = milliseconds(obj['ts'])
        if channel == 'tickers' and market == 'bybit':
            yield 'derivative_ticker', self.ticker(key, ts, self.ticker_fields(data))
        elif channel in ('orderbook.1', 'orderbook.50'):
            if obj['type'] not in ('snapshot', 'delta'):
                raise ValueError('invalid book action')
            state = self.channels.setdefault(key, {})
            snapshot = obj['type'] == 'snapshot'
            if channel == 'orderbook.1':
                yield 'book_ticker', self.top(state, snapshot, data, ts)
            else:
                yield 'quotes', self.depth(state, snapshot, data, ts)

    def depth(self, state, snapshot, data, ts):
        # 更新 ID 可跳號，較晚抵達的舊資料不應回滾簿；服務重置採用新的完整快照。
        if 'book' not in state:
            state['book'] = OrderBook()
        book = state['book']
        sequence = int(data['u'])
        if book.ready and (ts < book.timestamp or (not snapshot and sequence <= book.sequence)):
            return None
        if not snapshot and not book.ready:
            self.issues['depth_before_snapshot'] += 1
            return None
        row = book.apply(snapshot, data['b'], data['a'], ts, sequence)
        return self.changed_quote(state, row)

    @staticmethod
    def top(state, snapshot, data, ts):
        # 一檔原生快照會清空缺側，增量則沿用該側上次的報價。
        if not snapshot and 'ask' not in state:
            return None
        if ts < state.get('timestamp', ts):
            return None
        for side, field in (('bid', 'b'), ('ask', 'a')):
            previous = (None, None) if snapshot else state[side]
            state[side] = next(((dec(p), dec(q)) for p, q in data[field] if dec(q) != 0), previous)
        state['timestamp'] = ts
        return best_row(ts, state['bid'], state['ask'])

    @staticmethod
    def ticker_fields(data):
        # 空字串不覆蓋先前資訊，資金時間取毫秒欄位 nextFundingTime。
        fields = {'lastPrice': 'last_price', 'openInterest': 'open_interest', 'indexPrice': 'index_price',
                  'markPrice': 'mark_price', 'fundingRate': 'funding_rate'}
        updates = {target: dec(data[k]) for k, target in fields.items() if data.get(k) not in (None, '')}
        if data.get('nextFundingTime') not in (None, ''):
            updates['funding_timestamp'] = milliseconds(data['nextFundingTime'])
        return updates
