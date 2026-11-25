"""供採集與離線重播共用的 bybit 原生協議。"""


def classify(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if not isinstance(obj, dict): return 'unexpected', None
    if obj.get('success') is False or obj.get('error'): return 'error', None
    if obj.get('op') == 'subscribe' and obj.get('success') is True: return 'ack', None
    if obj.get('topic') == feed['channel'] + '.' + feed['symbol'] and 'data' in obj: return 'data', obj
    return 'control', None
