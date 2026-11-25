"""供採集與離線重播共用的 okx 原生協議。"""


def classify(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if obj == 'pong': return 'control', None
    if not isinstance(obj, dict): return 'unexpected', None
    if obj.get('event') == 'error': return 'error', None
    arg = obj.get('arg', {})
    if arg.get('channel') == feed['channel'] and arg.get('instId') == feed['symbol']:
        if obj.get('event') == 'subscribe': return 'ack', None
        if 'data' in obj: return 'data', obj
    return 'control', None
