"""五家交易所共用的原生訊息分類、解碼與 Binance 欄位驗證。"""
import base64
from decimal import Decimal, InvalidOperation
import json

from common import ProtocolError, dec




ADAPTERS = {'binance': 'binance', 'binance-futures': 'binance', 'bybit': 'bybit',
            'bybit-spot': 'bybit', 'okex': 'okx', 'okex-swap': 'okx', 'coinbase': 'coinbase', 'upbit': 'upbit'}


def validate(data, channel, symbol):
    # 檢查 Binance 必要欄位、商品與數值；驗證失敗也不刪除已保存的原文。
    if not isinstance(data,dict): raise ProtocolError('payload is not object')
    try:
        d=data.get('o',{}) if channel=='forceOrder' else data
        if channel != 'depthSnapshot' and d.get('s',d.get('symbol')) != symbol:
            raise ProtocolError('symbol mismatch')
        if channel.startswith('depth'):
            if channel=='depthSnapshot':
                int(d['lastUpdateId']); arrays=[d['bids'],d['asks']]
            else:
                for k in ('U','u','E'): int(d[k])
                arrays=[d['b'],d['a']]
            for levels in arrays:
                if not isinstance(levels,list): raise ProtocolError('depth levels not array')
                for p,q in levels:
                    if dec(p)<=0 or dec(q)<0: raise ProtocolError('invalid level')
        elif channel=='bookTicker':
            for k in ('a','A','b','B'): dec(d[k])
            int(d['u'])
        elif channel=='trade':
            for k in ('t','T'): int(d[k])
            for k in ('p','q'): dec(d[k])
            if not isinstance(d['m'],bool): raise ProtocolError('invalid maker flag')
        elif channel=='ticker': dec(d['c']); int(d['E'])
        elif channel=='markPrice@1s':
            dec(d['p']); int(d['E'])
            for k in ('i','r'):
                if d.get(k) not in (None,''): dec(d[k])
            if 'T' in d: int(d['T'])
        elif channel=='forceOrder':
            for k in ('p','z'): dec(d[k])
            int(d['T'])
            if d['S'] not in ('BUY','SELL'): raise ProtocolError('invalid side')
            str(d['X'])
        elif channel=='openInterest': dec(d['openInterest'])
    except (KeyError,TypeError,ValueError,InvalidOperation) as exc:
        raise ProtocolError(f'{channel}: {type(exc).__name__}: {exc}') from exc
    return data


def decode(record):
    # 保留原始十進位精度，二進位 JSON 只在解析時解碼。
    raw = record['raw']
    if record.get('encoding') == 'base64':
        raw = base64.b64decode(raw, validate=True)
    return json.loads(raw, parse_float=Decimal)


def classify_binance(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if not isinstance(obj, dict): return 'unexpected', None
    if obj.get('id') == 1 and 'result' in obj:
        return ('ack' if obj['result'] is None else 'error'), None
    if 'error' in obj or ('code' in obj and 'msg' in obj): return 'error', None
    if obj.get('stream') == f'{feed["symbol"].lower()}@{feed["channel"]}' and isinstance(obj.get('data'), dict):
        return 'data', obj['data']
    return 'unexpected', None


def classify_bybit(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if not isinstance(obj, dict): return 'unexpected', None
    if obj.get('success') is False or obj.get('error'): return 'error', None
    if obj.get('op') == 'subscribe' and obj.get('success') is True: return 'ack', None
    if obj.get('topic') == feed['channel'] + '.' + feed['symbol'] and 'data' in obj: return 'data', obj
    return 'control', None


def classify_okx(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if obj == 'pong': return 'control', None
    if not isinstance(obj, dict): return 'unexpected', None
    if obj.get('event') == 'error': return 'error', None
    arg = obj.get('arg', {})
    if arg.get('channel') == feed['channel'] and arg.get('instId') == feed['symbol']:
        if obj.get('event') == 'subscribe': return 'ack', None
        if 'data' in obj: return 'data', obj
    return 'control', None


def classify_coinbase(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if not isinstance(obj, dict): return 'unexpected', None
    kind = obj.get('type')
    if kind == 'error': return 'error', None
    if kind == 'subscriptions':
        ack = any(c.get('name') == feed['channel'] and feed['symbol'] in c.get('product_ids', []) for c in obj.get('channels', []))
        return ('ack' if ack else 'control'), None
    expected = {'matches': ('match', 'last_match'), 'level2_batch': ('snapshot', 'l2update'), 'ticker': ('ticker',)}
    if obj.get('product_id') == feed['symbol'] and kind in expected[feed['channel']]: return 'data', obj
    return 'control', None


def classify_upbit(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if not isinstance(obj, dict): return 'unexpected', None
    if 'error' in obj: return 'error', None
    if obj.get('code') == feed['symbol'] and obj.get('type') == feed['channel']: return 'data', obj
    return 'control', None


CLASSIFIERS = {
    'binance': classify_binance, 'bybit': classify_bybit, 'okx': classify_okx,
    'coinbase': classify_coinbase, 'upbit': classify_upbit,
}


def classify(feed, payload):
    # 同一交易所的現貨與期貨共用原生訊息辨識。
    return CLASSIFIERS[ADAPTERS[feed['exchange']]](feed, payload)
