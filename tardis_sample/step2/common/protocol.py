"""依市場選擇原生訊息分類器，並提供精確十進位解碼。"""
import base64
from decimal import Decimal
import json
from . import binance, bybit, coinbase, okx, upbit
from .binance import validate

FAMILIES = {
    'binance': ['binance', 'binance-futures'], 'bybit': ['bybit', 'bybit-spot'],
    'okx': ['okex', 'okex-swap'], 'coinbase': ['coinbase'], 'upbit': ['upbit'],
}
ADAPTERS = {market: family for family, markets in FAMILIES.items() for market in markets}
CLASSIFIERS = {'binance': binance.classify, 'bybit': bybit.classify, 'okx': okx.classify,
               'coinbase': coinbase.classify, 'upbit': upbit.classify}


def decode(record):
    # 保留原始十進位精度，二進位 JSON 只在解析時解碼。
    raw = record['raw']
    if record.get('encoding') == 'base64':
        raw = base64.b64decode(raw, validate=True)
    return json.loads(raw, parse_float=Decimal)


def classify(feed, payload):
    # 同一交易所的現貨與期貨共用原生訊息辨識。
    return CLASSIFIERS[ADAPTERS[feed['exchange']]](feed, payload)
