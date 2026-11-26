"""Bybit linear／spot 原始採集協議；保留原生快照／增量物件，不套用 Binance 規則。"""
from common.bybit import classify
from ..transport import BaseCollector

class Collector(BaseCollector):
    """交易所專屬的來源與協議實作；傳輸、落盤與日誌沿用 BaseCollector。"""
    batch_field = 'data'
    unwrap_single = True
    optional_channels = ('allLiquidation',)

    @staticmethod
    def control_fields(feed, obj):
        # 應用層 pong 確認同組訂閱的傳輸健康。
        if obj.get('op') == 'pong' or obj.get('ret_msg') == 'pong':
            return {}

    replica_group_limit = 10
    # 僅列出具可靠原生事件身分的來源；身分不等於連續性保證。
    replica_strong_channels = ('publicTrade', 'orderbook.1', 'orderbook.50')
    continuity_channels = {}
    replica_silence_seconds = {'orderbook.1': 15}
    connection_limits = ((300, 500), (1, 10))
    @staticmethod
    def replica_limit(market):
        # 現貨單次訂閱最多十項；linear 使用較大批次減少大量商品的連線數。
        return 10 if market == 'bybit-spot' else 100
    application_heartbeat = '{"op":"ping"}'
    @staticmethod
    def default_symbols(market):
        # 現貨與線性永續的預設訂閱都使用 Bybit 的 USDT 商品代碼。
        return ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']

    @staticmethod
    def channels(market):
        # 列出該市場的原生來源；現貨與衍生品清單明確分開。
        return ['publicTrade', 'orderbook.50', 'orderbook.1', 'tickers'] + (['allLiquidation'] if market == 'bybit' else [])
    @staticmethod
    def endpoints(market):
        # 依市場回傳可用的原生 WebSocket 路由。
        category = 'spot' if market == 'bybit-spot' else 'linear'
        return [f'wss://stream.bybit.com/v5/public/{category}']
    classify = staticmethod(classify)

    @staticmethod
    def replica_subscription(feeds):
        # 現貨每組最多十個參數，其他市場亦採保守分組。
        return {'op': 'subscribe', 'args': [f['channel'] + '.' + f['symbol'] for f in feeds]}

    @staticmethod
    def replica_symbol(obj, channel):
        # topic 同時包含來源與商品；先核對來源再取商品。
        topic = obj.get('topic', '') if isinstance(obj, dict) else ''
        return topic[len(channel) + 1:] if topic.startswith(channel + '.') else None

    @staticmethod
    def replica_identity(channel, data, envelope):
        # publicTrade 同一 seq 可含多筆成交，必須以每筆 i 去重。
        from ..replica_protocol import stream_marker
        kind = envelope.get('type', '')
        if channel == 'publicTrade': return ('trade', data['i']), True, None, False
        if channel.startswith('orderbook.'):
            snapshot = kind == 'snapshot'
            marker = stream_marker(channel, data['u'], reset=snapshot or data['u'] == 1)
            # level1 可定期重發同 u 的快照；發布時間不同仍是不同原生訊息。
            publication = envelope.get('ts') if snapshot else None
            ident = (kind, data['u'], data.get('seq'), envelope.get('cts'), publication)
            return ident, not snapshot or publication is not None, marker, snapshot
        if channel == 'tickers':
            return (kind, envelope.get('cs'), envelope.get('ts')), False, None, kind == 'snapshot'
        return (channel, data.get('T'), envelope.get('ts')), False, None, False
