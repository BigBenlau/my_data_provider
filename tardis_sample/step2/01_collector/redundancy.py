"""雙副本共用設定、版本識別與分組；所有角色使用同一程式及映像。"""
import hashlib
import json
from threading import TIMEOUT_MAX
from pathlib import Path
from collections import defaultdict
from common import STEP2_ROOT
from common.protocol import FAMILIES
from .transport import COLLECTOR_ROOT, adapter, configuration, key

DEFAULTS = {
    'rotation_seconds': 600, 'verify_seconds': 10, 'match_events': 3,
    'match_wait_seconds': 5, 'candidate_timeout': 120, 'heartbeat_seconds': 5,
    'worker_timeout': 20, 'drain_timeout': 60,
    'segment_seconds': 60, 'spool_max_bytes': 2 * 1024**3, 'min_free_gib': 2,
    'batch_size': 2048, 'flush_seconds': .25, 'group_size': 100,
    'docker_socket': '/var/run/docker.sock', 'docker_api': 'v1.45',
    'ws_attempts_per_5min': 150, 'rest_concurrency': 6,
    'repair_grace_seconds': 15, 'repair_cooldown_seconds': 60,
    'index_hot_seconds': 86400, 'archive_batch_size': 4096, 'archive_seconds': 1,
    'compress_raw': True,
    'batch_max_bytes': 16 * 1024**2,
}


def stable_json(value):
    # 只正規化封裝與索引；原生 raw 字串不重新排版。
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str)


def fingerprint(value):
    # 用固定序列化建立設定或事件指紋，不使用不穩定的 Python hash。
    return hashlib.sha256(stable_json(value).encode()).hexdigest()


def code_version():
    # 主管與 worker 以同一份採集程式指紋確認版本。
    return fingerprint(code_hashes())


def code_hashes():
    # 只納入第 01 階段及共用程式，並以相對路徑在主機與映像內重現。
    paths = [*COLLECTOR_ROOT.glob('*.py'), *(COLLECTOR_ROOT / 'collectors').glob('*.py'),
             *(STEP2_ROOT / 'common').glob('*.py')]
    return {str(path.relative_to(STEP2_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(paths)}


def group_config(family, path=None):
    # 將舊單市場設定及新每市場清單轉為同一份可封存設定。
    if family not in FAMILIES:
        raise ValueError('unknown exchange family')
    source = json.loads((Path(path) if path else COLLECTOR_ROOT / 'settings.json').read_text())
    selected = source.get('exchanges', {}).get(family, {})
    markets = selected.get('markets', {})
    if not markets:
        markets = {m: {'symbols': adapter(m).legacy_symbols(source, m)}
                   for m in FAMILIES[family]}
    if not markets or not set(markets) <= set(FAMILIES[family]):
        raise ValueError('markets must belong to one exchange family')
    result = dict(source, family=family, markets=list(markets), market_settings=markets)
    result.pop('exchanges', None)
    result['redundancy'] = {**DEFAULTS, **source.get('redundancy', {}), **selected.get('redundancy', {})}
    result['metadata'] = {}
    for market, settings in markets.items():
        validate_market(result, market, settings)
    for name in DEFAULTS:
        if type(DEFAULTS[name]) in (int, float) and result['redundancy'][name] <= 0:
            raise ValueError(f'redundancy.{name} must be positive')
    if not isinstance(result['redundancy']['compress_raw'], bool): raise ValueError('compress_raw must be boolean')
    if type(result['redundancy']['group_size']) is not int: raise ValueError('group_size must be a positive integer')
    wait = result['redundancy'].get('spool_capacity_wait_seconds', 10)
    if type(wait) not in (int, float) or not 0 <= wait <= TIMEOUT_MAX:
        raise ValueError('spool_capacity_wait_seconds must be a nonnegative supported timeout')
    if result['redundancy']['verify_seconds'] < 10 or result['redundancy']['match_events'] < 3:
        raise ValueError('handover requires at least 10 seconds and three common events')
    result['symbols'] = sorted({s for m in markets.values() for s in m['symbols']})
    result['config_hash'] = fingerprint(result)
    return result


def validate_market(config, market, settings):
    # 商品代碼與 channel 按市場檢查；不把其他市場代碼默默轉換。
    symbols = settings.get('symbols', adapter(market).default_symbols(market))
    channels = settings.get('channels', adapter(market).channels(market))
    if not symbols or len(set(symbols)) != len(symbols):
        raise ValueError('empty or duplicate symbols')
    if any(not s.isascii() or not s.replace('-', '').isalnum() for s in symbols):
        raise ValueError('invalid symbol')
    if not channels or len(set(channels)) != len(channels) or not set(channels) <= set(adapter(market).channels(market)):
        raise ValueError('unknown or duplicate channel')
    # 新報價幣的連寫商品可明確登記基礎資產，分組時無須額外連線查詢。
    overrides = settings.get('base_assets', {})
    if not isinstance(overrides, dict) or not set(overrides) <= set(symbols):
        raise ValueError(f'{market}: base_assets must map configured symbols to assets')
    if any(not isinstance(asset, str) or not asset.strip() for asset in overrides.values()):
        raise ValueError(f'{market}: invalid base asset')
    settings.update(symbols=symbols, channels=channels)
    local = dict(config, symbols=symbols, managed=True)
    adapter(market).configure(local, market)
    config['metadata'][market]['continuity_verification'] = adapter(market).continuity_channels
    config['metadata'][market]['silence_seconds'] = adapter(market).replica_silence_seconds


def configured_feeds(config):
    # 展開所有選取的 WS 來源；REST 來源由共用排程建立。
    result = []
    for market, settings in config['market_settings'].items():
        local = dict(config, symbols=settings['symbols'])
        for feed in adapter(market).feeds(local, market):
            if feed['channel'] in settings['channels']:
                result.append(feed)
    return result


def feed_groups(config):
    # 在同一市場、來源及路由中，BTC、ETH 與其餘商品分別建立連線。
    buckets = defaultdict(list)
    for feed in configured_feeds(config):
        market, symbol = feed['exchange'], feed['symbol']
        overrides = config['market_settings'][market].get('base_assets', {})
        asset = overrides.get(symbol) or adapter(market).ws_asset(symbol.upper())
        bucket = asset.upper() if asset and asset.upper() in ('BTC', 'ETH') else 'others'
        buckets[(market, feed['channel'], tuple(feed['urls']), bucket)].append(feed)
    groups = []
    for (market, channel, urls, bucket), items in sorted(buckets.items()):
        # 每組獨立套用訂閱上限；固定排序讓所有副本產生相同的訂閱與修復目標。
        size = min(config['redundancy']['group_size'], adapter(market).replica_limit(market))
        items.sort(key=lambda f: f['symbol'])
        for start in range(0, len(items), size):
            members = items[start:start + size]
            groups.append({'id': f'{market}/{channel}/{bucket}/{start // size}', 'feeds': members,
                           'urls': list(urls), 'lookup': {f['symbol']: f for f in members}})
    return groups


def source_inventory(config):
    # 預建來源清單，讓安靜的清算與沒有收到資料的來源仍出現在狀態中。
    return {f'{market}/{symbol}/{channel}': dict(exchange=market, symbol=symbol, channel=channel)
            for market, settings in config['market_settings'].items()
            for symbol in settings['symbols'] for channel in settings['channels']}


def preflight(config):
    # 不連線的容量與嚴格換代檢查；交易所即時限額在正式啟動後再核對。
    import tempfile
    from .rest_schedule import Schedule
    blocked = {key(f): 'no_reliable_native_identity' for f in configured_feeds(config)
               if f['channel'] not in adapter(f['exchange']).replica_strong_channels}
    with tempfile.TemporaryDirectory() as root:
        schedule = Schedule(Path(root) / 'schedule.sqlite', config)
        try: capacity = schedule.capacity_summary()
        finally: schedule.close()
    return dict(handover_policy='strict', variant_policy='preserve_originals',
                rotation_blockers=blocked, rest_capacity=capacity, limits_verified_online=False)


def identity(config, image):
    # 不包含 worker 名稱與暫存路徑，使 A/B/C 的驗證身分完全相同。
    return {'config_hash': config['config_hash'], 'code_hash': code_version(), 'image': image}


def local_paths(root):
    # 控制 socket 使用短路徑；Docker 與本機測試共用相同目錄結構。
    root = Path(root).resolve()
    socket = root / '_state' / 'ingest.sock'
    if len(str(socket).encode()) > 90: socket = Path('/tmp') / ('collector-' + fingerprint(str(root))[:24] + '.sock')
    return {'root': root, 'control': root / '_state', 'socket': socket,
            'spool': root / '_spool', 'raw': root / 'raw'}
