"""供採集與離線重播共用的 coinbase 原生協議。"""


def classify(feed, obj):
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
