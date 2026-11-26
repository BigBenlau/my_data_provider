"""副本協議工具：批次拆分與原生事件識別；不建立價位狀態。"""
import hashlib
import json
from decimal import Decimal
from common.protocol import decode
from .transport import adapter
from .raw import json_text


RULE_VERSION = 2


def native_text(value):
    # 排序物件鍵；數字保持 JSON 數值型別及完整精度，陣列維持原順序。
    if isinstance(value, dict):
        return '{' + ','.join(json.dumps(k) + ':' + native_text(value[k]) for k in sorted(value)) + '}'
    if isinstance(value, (list, tuple)):
        return '[' + ','.join(native_text(v) for v in value) + ']'
    if isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        if not any(digits):
            return '0'
        digits = ''.join(map(str, digits))
        trimmed = digits.rstrip('0')
        exponent += len(digits) - len(trimmed)
        if exponent >= 0 and len(trimmed) + exponent < 5000:
            return ('-' if sign else '') + trimmed + '0' * exponent
        return ('-' if sign else '') + trimmed + 'e' + str(exponent)
    return json.dumps(value, allow_nan=False)


def native_hash(value):
    # 大多數交易所以字串報價，標準編碼器可快速完成相同的排序。
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
    except TypeError:
        encoded = native_text(value)
    return hashlib.sha256(encoded.encode()).hexdigest()


def atoms(record):
    # 只讓真實行情進入正式輸出；控制訊息由 worker 日誌處理。
    obj = decode(record)
    market, channel = record['exchange'], record['channel']
    cls = adapter(market)
    if record['kind'] == 'rest':
        cls.validate_event(obj, channel, record['symbol'])
        ident = record['request_id']
        return [dict(record=record, ident=ident, digest=native_hash(obj), strong=True,
                     marker=None, snapshot=cls.rest_policy is not None and channel == cls.rest_policy.snapshot)]
    kind, data = cls.classify(record, obj)
    if kind != 'data':
        raise ValueError('not a matching data message')
    cls.validate_event(data, channel, record['symbol'])
    values = data.get(cls.batch_field) if cls.batch_field else None
    split = isinstance(values, list)
    result = []
    single = values if cls.unwrap_single and isinstance(values, dict) else data
    for index, value in enumerate(values if split else [single]):
        if not isinstance(value, dict):
            raise ValueError('native event must be object')
        ident, strong, marker, snapshot = cls.replica_identity(channel, value, obj)
        item = dict(record)
        native = {**obj, cls.batch_field: [value]} if split else obj
        if split:
            item.update(raw=json_text(native), encoding='utf8',
                        batch_split_version=1, batch_index=index, batch_size=len(values))
        result.append(dict(record=item, ident=native_hash(ident), digest=native_hash(native),
                           strong=strong, marker=marker, snapshot=snapshot, variant=variant_signature(record, native)))
    return result


def variant_signature(record, native):
    # 差異分類只作診斷；不改原文、去重鍵或嚴格交接條件。
    market, channel = record['exchange'], record['channel']
    rule = adapter(market).variant_fields.get((market, channel))
    cls = adapter(market)
    fields, category = rule or (cls.publication_fields, 'publication_timestamp_variant')
    if not fields:
        return None
    name = cls.publication_object
    base = native.get(name) if name else native
    if not isinstance(base, dict):
        return None
    cleaned = {k: v for k, v in base.items() if k not in fields}
    return dict(category=category, digest=native_hash({**native, name: cleaned} if name else cleaned))


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


class Decoder:
    def __init__(self):
        # 耐久去重仍查完整索引；小型快取只重用逐位元相同封包的解析結果。
        from collections import OrderedDict
        self.recent = OrderedDict()

    def events(self, record):
        # 只快取短小的單筆 WS。接收時間、session 與本次交付序號每次重新附上。
        raw = record['raw']
        if record['kind'] != 'ws' or len(raw) > 1024:
            return atoms(record)
        key = (record['exchange'],record['symbol'],record['channel'],record.get('encoding'),raw)
        cached = self.recent.get(key)
        if cached is not None:
            self.recent.move_to_end(key)
            return [dict(cached,record=record)]
        events = atoms(record)
        if len(events)==1 and 'batch_split_version' not in events[0]['record']:
            self.recent[key] = {k:v for k,v in events[0].items() if k!='record'}
            if len(self.recent)>2048:
                self.recent.popitem(last=False)
        return events
