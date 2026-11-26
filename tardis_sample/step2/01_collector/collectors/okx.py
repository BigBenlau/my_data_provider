"""OKX swap／spot 公開頻道協議；trades-all 與清算 adapter 仍待另外實作。"""
from common.okx import classify
from ..transport import BaseCollector

class Collector(BaseCollector):
    """交易所專屬的來源與協議實作；傳輸、落盤與日誌沿用 BaseCollector。"""
    publication_fields = {'ts'}
    batch_field = 'data'

    application_heartbeat = 'ping'
    text_heartbeat_replies = ('pong', b'pong')
    # 僅列出具可靠原生事件身分的來源；身分不等於連續性保證。
    replica_strong_channels = ('trades', 'books', 'bbo-tbt')
    continuity_channels = {'books': 'prevSeqId/seqId with reset'}
    replica_silence_seconds = {'books': 65}
    connection_limits = ((1, 2),)
    @staticmethod
    def default_symbols(market):
        # 回傳該市場的原生商品代碼，避免把另一交易所的代碼直接套入。
        return [s + '-USDT' + ('-SWAP' if market == 'okex-swap' else '') for s in ('BTC', 'ETH', 'SOL')]
    @staticmethod
    def channels(market):
        # 列出該市場的原生來源；現貨與衍生品清單明確分開。
        return ['trades', 'books', 'bbo-tbt', 'tickers'] + (['mark-price', 'funding-rate', 'open-interest'] if market == 'okex-swap' else [])
    @staticmethod
    def endpoints(market):
        # 依市場回傳可用的原生 WebSocket 路由。
        return ['wss://ws.okx.com/ws/v5/public', 'wss://ws.okx.com:8443/ws/v5/public']
    classify = staticmethod(classify)

    @staticmethod
    def replica_subscription(feeds):
        # 每個 arg 保留自己的市場商品代碼，ACK 也逐 arg 核對。
        return {'op': 'subscribe', 'args': [{'channel': f['channel'], 'instId': f['symbol']} for f in feeds]}

    @staticmethod
    def replica_symbol(obj, channel):
        # 按 arg 路由，文字 pong 由共用傳輸先行處理。
        return obj.get('arg', {}).get('instId') if isinstance(obj, dict) else None

    @staticmethod
    def replica_identity(channel, data, envelope):
        # seqId 可以重置，時間及 action 納入身分；保留 prevSeqId 銜接證據。
        from ..replica_protocol import stream_marker
        if channel == 'trades': return ('trade', data['tradeId']), True, None, False
        snapshot = envelope.get('action') == 'snapshot'
        if 'seqId' in data:
            reset = snapshot or int(data['seqId']) < int(data.get('prevSeqId', -1))
            marker = stream_marker(channel, data['seqId'], data.get('prevSeqId'), reset=reset)
            return (envelope.get('action'), data['seqId'], data.get('ts')), True, marker, snapshot
        return (channel, data.get('ts'), data.get('fundingTime')), False, None, snapshot
