"""批次下載 Tardis CSV：pip install tardis-dev。

先編輯 EXCHANGE_CONFIG，再設定環境變數 TARDIS_API_KEY。
輸出路徑固定以本腳本所在資料夾為基準，不受執行時工作目錄影響。
"""
import logging
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "datasets"
FROM_DATE = "2026-09-18"
TO_DATE = "2026-09-19"  # 不包含此日期
TARDIS_API_KEY = os.environ.get("TARDIS_API_KEY", "")

# 每個 exchange 分別設定商品代碼與資料類型。
# 請確認指定日期有提供這些商品／類型：
# https://api.tardis.dev/v1/exchanges/<exchange>
EXCHANGE_CONFIG = {
    "binance-futures": {
        "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "GRTUSDT"],
        "data_types": ["trades", "quotes", "book_ticker",
                       "derivative_ticker", "liquidations"],
    },
    "bybit": {
        "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "GRTUSDT"],
        "data_types": ["trades", "quotes", "book_ticker", "derivative_ticker", "liquidations"],
    },
    "okex-swap": {
        "symbols": ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "GRT-USDT-SWAP"],
        "data_types": ["trades", "quotes", "book_ticker",
                       "derivative_ticker", "liquidations"],
    },
    # 現貨：前三家使用 USDT，Coinbase 使用 USD，Upbit 使用 KRW。
    "binance": {
        "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "GRTUSDT"],
        "data_types": ["trades", "quotes", "book_ticker"],
    },
    "bybit-spot": {
        "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "GRTUSDT"],
        "data_types": ["trades", "quotes", "book_ticker"],
    },
    "okex": {
        "symbols": ["BTC-USDT", "ETH-USDT", "SOL-USDT", "GRT-USDT"],
        "data_types": ["trades", "quotes", "book_ticker"],
    },
    "coinbase": {
        "symbols": ["BTC-USD", "ETH-USD", "SOL-USD", "GRT-USD"],
        "data_types": ["trades", "quotes", "book_ticker"],
    },
    "upbit": {
        "symbols": ["KRW-BTC", "KRW-ETH", "KRW-SOL", "KRW-GRT"],
        # Upbit 沒有獨立的 book_ticker dataset。
        "data_types": ["trades", "quotes"],
    },
}


def file_name_nested(exchange, data_type, date, symbol, format):
    """下載檔案的相對路徑：exchange/data_type/date_symbol.csv.gz。"""
    return f"{exchange}/{data_type}/{date:%Y-%m-%d}_{symbol}.{format}.gz"


def download_exchanges(config, from_date, to_date, download_dir, api_key, download_fn=None):
    """依序下載所有 exchange；某個失敗仍繼續其他項，最後回報整批失敗。"""
    # 先驗證整份設定，避免處理一半才發現空設定。
    if not config:
        raise ValueError("至少需設定一個 exchange")
    for exchange, options in config.items():
        for name in ("symbols", "data_types"):
            values = options.get(name)
            if not isinstance(values, list) or not values or not all(
                isinstance(value, str) and value.strip() for value in values
            ):
                raise ValueError(f"{exchange}: {name} 必須是非空字串 list")
    if download_fn is None:
        from tardis_dev import datasets
        download_fn = datasets.download
    failures = []
    for exchange, options in config.items():
        logging.info("下載 %s，symbols=%s", exchange, options["symbols"])
        try:
            download_fn(
                exchange=exchange,
                data_types=options["data_types"],
                from_date=from_date,
                to_date=to_date,
                symbols=options["symbols"],
                api_key=api_key,
                download_dir=str(download_dir),
                get_filename=file_name_nested,
            )
        except Exception:
            failures.append(exchange)
            logging.exception("下載失敗：%s", exchange)
    if failures:
        raise RuntimeError(f"以下 exchange 下載失敗：{', '.join(failures)}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    download_exchanges(EXCHANGE_CONFIG, FROM_DATE, TO_DATE, DOWNLOAD_DIR, TARDIS_API_KEY)


if __name__ == "__main__":
    main()
