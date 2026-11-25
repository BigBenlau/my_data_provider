"""副本協議工具：批次拆分與原生事件識別；不建立價位狀態。"""
from common.protocol import decode, validate
from .transport import adapter
from .raw import json_text
from .redundancy import fingerprint


def atoms(record):
    # 只讓真實行情進入正式輸出；控制訊息由 worker 日誌處理。
    obj = decode(record)
    market, channel = record['exchange'], record['channel']
    if record['kind'] == 'rest':
        validate(obj, channel, record['symbol'])
        ident = record['request_id']
        return [dict(record=record, ident=ident, digest=fingerprint(obj), strong=True,
                     marker=None, snapshot=channel == 'depthSnapshot')]
    cls = adapter(market)
    kind, data = cls.classify(record, obj)
    if kind != 'data':
        raise ValueError('not a matching data message')
    if market.startswith('binance'):
        validate(data, channel, record['symbol'])
    values = data.get('data') if isinstance(data, dict) else None
    split = isinstance(values, list) and market in ('bybit', 'bybit-spot', 'okex', 'okex-swap')
    result = []
    single = values if market in ('bybit', 'bybit-spot') and isinstance(values, dict) else data
    for index, value in enumerate(values if split else [single]):
        if not isinstance(value, dict):
            raise ValueError('native event must be object')
        ident, strong, marker, snapshot = cls.replica_identity(channel, value, obj)
        item = dict(record)
        if split:
            item.update(raw=json_text(dict(obj, data=[value])), encoding='utf8',
                        batch_split_version=1, batch_index=index, batch_size=len(values))
        result.append(dict(record=item, ident=fingerprint(ident), digest=fingerprint(value),
                           strong=strong, marker=marker, snapshot=snapshot, variant=variant_signature(record, value)))
    return result


def variant_signature(record, value):
    # 差異分類只作診斷；不改原文、去重鍵或嚴格交接條件。
    market, channel = record['exchange'], record['channel']
    if market == 'binance' and channel == 'depth@100ms':
        fields, category = {'E'}, 'publication_timestamp_variant'
    elif market == 'coinbase' and channel == 'ticker':
        fields, category = {'volume_24h', 'volume_30d'}, 'rolling_statistics_variant'
    else: return None
    return dict(category=category, digest=fingerprint({k: v for k, v in value.items() if k not in fields}))


def subscription(group):
    # 每組只送一份相同訂閱；各交易所決定自己的封裝與限制。
    return adapter(group['feeds'][0]['exchange']).replica_subscription(group['feeds'])


def classify_group(group, obj):
    # 從原生路由直接定位商品，避免每筆資料掃描所有訂閱。
    sample = group['feeds'][0]
    cls = adapter(sample['exchange'])
    symbol = cls.replica_symbol(obj, sample['channel'])
    if symbol in group['lookup']:
        feed = group['lookup'][symbol]
        kind, _ = cls.classify(feed, obj)
        return kind, [feed]
    acknowledged = []
    for feed in group['feeds']:
        kind, _ = cls.classify(feed, obj)
        if kind == 'error':
            return 'error', []
        if kind == 'ack':
            acknowledged.append(feed)
    return ('ack', acknowledged) if acknowledged else ('control', [])


def stream_marker(channel, last, previous=None, first=None, reset=False):
    # 僅封裝序號觀察值；沒有明確前序語義時不推論整數連續。
    return {'u': int(last), 'previous': int(previous) if previous is not None else None,
            'first': int(first) if first is not None else None, 'reset': reset}
