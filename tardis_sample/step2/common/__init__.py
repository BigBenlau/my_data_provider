"""三階段共用的路徑、Tardis 欄位、數值、時間與日誌。"""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path

STEP2_ROOT = Path(__file__).resolve().parents[1]
BASE = ['exchange','symbol','timestamp','local_timestamp']
BBO = ['ask_amount','ask_price','bid_price','bid_amount']
TICKER = ['funding_timestamp','funding_rate','predicted_funding_rate','open_interest','last_price','index_price','mark_price']
SCHEMAS = {'trades':BASE+['id','side','price','amount'], 'quotes':BASE+BBO, 'book_ticker':BASE+BBO,
           'liquidations':BASE+['id','side','price','amount'], 'derivative_ticker':BASE+TICKER}
TYPES = {'binance':['trades','quotes','book_ticker'], 'binance-futures':list(SCHEMAS)}

class ProtocolError(ValueError): 
    """來源 payload 格式不符合該 channel 的必要欄位。"""
    pass



def dec(value):
    # 以字串建立 Decimal，拒絕 NaN／Infinity，避免價格與數量的浮點誤差。
    try:
        n = Decimal(str(value))
    except InvalidOperation as exc:
        raise ProtocolError('invalid decimal') from exc
    if not n.is_finite(): raise ProtocolError('non-finite decimal')
    return n


def encode(value):
    # 序列化正規化 CSV 數值；缺失欄位留空，Decimal 移除無意義的尾端零。
    if value is None: return ''
    if isinstance(value,Decimal):
        s = format(value,'f')
        return s.rstrip('0').rstrip('.') if '.' in s else s
    return str(value)


def micros(value, unit): 
    # 依明確的來源單位換算 epoch 微秒，不使用數值大小猜測單位。
    return int(value) * (1 if unit == 'us' else 1000)
def utc(us, fmt='%Y-%m-%d'): 
    # 將 epoch 微秒格式化為 UTC 日期或檔名時間。
    return datetime.fromtimestamp(us / 1e6, timezone.utc).strftime(fmt)


def atomic_json(path, data):
    # 先寫臨時檔並 fsync，再原子替換，讓監控端不會讀到半份 JSON。
    path = Path(path)
    tmp = path.with_name(path.name + '.tmp')
    with tmp.open('w',encoding='utf8') as f:
        json.dump(data,f,ensure_ascii=False,indent=2); f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)


LOG = logging.getLogger('market-data')


class TimestampFormatter(logging.Formatter):
    """為每一行日誌加上主機本地時間、毫秒及 UTC offset，包含 traceback 行。"""
    def format(self, record):
        # 透過標準 logging Formatter 產生內容，再逐行補上統一的 [datetime] 前綴。
        stamp = datetime.fromtimestamp(record.created).astimezone().isoformat(timespec='milliseconds')
        # traceback 每行也帶時間，避免多行錯誤失去所屬事件與時區。
        return '\n'.join(f'[{stamp}] {line}' for line in super().format(record).splitlines())


def configure_logging(path=None, max_bytes=10 * 1024 * 1024, backups=5):
    # 設定單一終端 handler，並可加上有大小與備份上限的日誌檔 handler。
    LOG.setLevel(logging.INFO)
    LOG.propagate = False
    formatter = TimestampFormatter('%(levelname)s %(message)s')
    if not any(getattr(h, '_market_console', False) for h in LOG.handlers):
        console = logging.StreamHandler()
        console._market_console = True
        console.setFormatter(formatter)
        LOG.addHandler(console)
    if path is not None:
        handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backups, encoding='utf8')
        handler.setFormatter(formatter)
        LOG.addHandler(handler)
        return handler


def close_log(handler):
    # 移除並關閉指定檔案 handler，避免多次 run 重複寫入舊日誌。
    if handler is not None:
        LOG.removeHandler(handler)
        handler.close()
