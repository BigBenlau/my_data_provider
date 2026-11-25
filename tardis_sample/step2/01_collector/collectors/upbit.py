"""Upbit 韓國市場協議：KRW 商品代碼、binary JSON 與沒有訂閱 ACK 的接入方式。"""
import uuid
from common.protocol import classify_upbit
from ..transport import BaseCollector

class Collector(BaseCollector):
    """交易所專屬的來源與協議實作；傳輸、落盤與日誌沿用 BaseCollector。"""
    requires_ack = False
    # 僅列出具可靠原生事件身分的來源；身分不等於連續性保證。
    replica_strong_channels = ('trade',)
    continuity_channels = {}
    replica_silence_seconds = {}
    connection_limits = ((1, 4),)
    @staticmethod
    def ws_asset(symbol):
        # Upbit 使用報價幣在前的代碼，BTC-SOL 的基礎資產是 SOL。
        return symbol.partition('-')[2]

    @staticmethod
    def default_symbols(market): 
        # 回傳該市場的原生商品代碼，避免把另一交易所的代碼直接套入。
        return ['KRW-' + s for s in ('BTC', 'ETH', 'SOL')]
    @staticmethod
    def channels(market): 
        # 列出該市場的原生來源；現貨與衍生品清單明確分開。
        return ['trade', 'orderbook']
    @classmethod
    def feeds(cls, config, market):
        # 展開每個商品／channel 的端點與時間單位，每個來源各自建立連線。
        return [dict(exchange=market, symbol=s, channel=c, time_unit='ms', urls=['wss://api.upbit.com/websocket/v1'])
                for s in config['symbols'] for c in cls.channels(market)]
    def subscription(self, feed):
        # 建立此交易所要求的訂閱訊息，不假設其他交易所具有相同格式。
        return [{'ticket': str(uuid.uuid4())}, {'type': feed['channel'], 'codes': [feed['symbol']]}, {'format': 'DEFAULT'}]
    classify = staticmethod(classify_upbit)

    @staticmethod
    def replica_subscription(feeds):
        # ticket 是執行期識別，不改變 A/B 的採集設定。
        return [{'ticket': str(uuid.uuid4())}, {'type': feeds[0]['channel'], 'codes': [f['symbol'] for f in feeds]},
                {'format': 'DEFAULT'}]

    @staticmethod
    def replica_symbol(obj, channel):
        # binary JSON 解碼後按原生 code 路由。
        return obj.get('code') if isinstance(obj, dict) else None

    @staticmethod
    def replica_identity(channel, data, envelope):
        # sequential_id 是成交身分，不能當作必須連續的整數序號。
        kind = data.get('stream_type', 'REALTIME')
        if channel == 'trade': return (kind, data['sequential_id']), True, None, kind == 'SNAPSHOT'
        return (kind, data.get('timestamp')), False, None, kind == 'SNAPSHOT'
