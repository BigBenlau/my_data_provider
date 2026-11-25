"""供採集與離線重播共用的 binance 原生協議。"""
from decimal import InvalidOperation
from . import ProtocolError, dec


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


def classify(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if not isinstance(obj, dict): return 'unexpected', None
    if obj.get('id') == 1 and 'result' in obj:
        return ('ack' if obj['result'] is None else 'error'), None
    if 'error' in obj or ('code' in obj and 'msg' in obj): return 'error', None
    if obj.get('stream') == f'{feed["symbol"].lower()}@{feed["channel"]}' and isinstance(obj.get('data'), dict):
        return 'data', obj['data']
    return 'unexpected', None
