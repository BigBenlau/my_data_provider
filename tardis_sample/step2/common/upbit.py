"""供採集與離線重播共用的 upbit 原生協議。"""


def classify(feed, obj):
    # 依本交易所協議辨識 ACK、控制、錯誤與對應商品資料；不修改原文。
    if not isinstance(obj, dict): return 'unexpected', None
    if 'error' in obj: return 'error', None
    if obj.get('code') == feed['symbol'] and obj.get('type') == feed['channel']: return 'data', obj
    return 'control', None
