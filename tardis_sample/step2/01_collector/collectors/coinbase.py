"""Coinbase Exchange 公開頻道協議；保留 matches／last_match 與 level2_batch 語義。"""
from common.coinbase import classify
from ..transport import BaseCollector

class Collector(BaseCollector):
    """交易所專屬的來源與協議實作；傳輸、落盤與日誌沿用 BaseCollector。"""
    time_unit = 'iso8601'
    probe_channels = ('ticker',)
    variant_fields = {('coinbase', 'ticker'): ({'volume_24h', 'volume_30d'}, 'rolling_statistics_variant')}

    @staticmethod
    def control_fields(feed, obj):
        # 心跳內的交易序號作為觀察資訊，僅更新相符商品。
        if obj.get('type') == 'heartbeat' and feed['symbol'] == obj.get('product_id'):
            return dict(heartbeat_trade_id=obj.get('last_trade_id'), heartbeat_sequence=obj.get('sequence'))

    # 僅列出具可靠原生事件身分的來源；身分不等於連續性保證。
    replica_strong_channels = ('matches', 'ticker')
    continuity_channels = {}
    replica_silence_seconds = {}
    connection_limits = ((1, 8),)
    @staticmethod
    def default_symbols(market): 
        # 回傳該市場的原生商品代碼，避免把另一交易所的代碼直接套入。
        return [s + '-USD' for s in ('BTC', 'ETH', 'SOL')]
    @staticmethod
    def channels(market): 
        # 列出該市場的原生來源；現貨與衍生品清單明確分開。
        return ['matches', 'level2_batch', 'ticker']
    @staticmethod
    def endpoints(market):
        # 依市場回傳可用的原生 WebSocket 路由。
        return ['wss://ws-feed.exchange.coinbase.com']
    classify = staticmethod(classify)

    @staticmethod
    def replica_subscription(feeds):
        # heartbeat 只用於來源監控，正式輸出仍維持既有 channel。
        return {'type': 'subscribe', 'product_ids': [f['symbol'] for f in feeds],
                'channels': [feeds[0]['channel'], 'heartbeat']}

    @staticmethod
    def replica_symbol(obj, channel):
        # 同一商品可能有 heartbeat，實際訊息類別仍由 classify 檢查。
        return obj.get('product_id') if isinstance(obj, dict) else None

    @staticmethod
    def replica_identity(channel, data, envelope):
        # matches 與初始化 last_match 分開；level2_batch 無可靠連續序號。
        kind = data['type']
        if channel == 'matches': return (kind, data['trade_id']), True, None, False
        if channel == 'ticker' and 'sequence' in data:
            return (kind, data['sequence']), True, None, False
        return (kind, data.get('time')), False, None, kind == 'snapshot'
