# Step2 行情採集、正規化與對帳

Step2 支援 Binance、Bybit、OKX、Coinbase 和 Upbit。先將各交易所的原生行情保存為可重播的 JSONL，再依交易所規則轉為 Tardis 格式，最後用同一市場、商品與時間窗的參考資料對帳。

| 階段 | 輸入 | 產出 | 操作說明 |
|---|---|---|---|
| 01 原始採集 | 各市場的 WebSocket 與已設定的 REST 來源 | 按 UTC 接收日分區的原生 JSONL，可匯出每日 JSONL 及各 channel 的每日 CSV.gz | [採集與監控](01_collector/README.md) |
| 02 Tardis 正規化 | 01 原始批次，可在採集中處理已過去的 UTC 日期 | 依市場、資料類型與事件日期分檔的 CSV，以及 `normalization_report.json` | [離線重播與正規化](02_normalize_tardis/README.md) |
| 03 對帳 | 02 的 CSV 與同時間窗的 Tardis 參考資料 | `comparison.json` | [對帳指令](RUN_01_02.md#使用-03-對帳) |

三個階段各自使用所屬目錄下的 `output/`。01 的 JSONL 是重播基準，02 直接讀取原始分段。03 使用的 Tardis 參考資料另放在 `../datasets/`。共用的讀取與協議處理位於 [common/](common/)，整體回歸測試位於 [tests/](tests/)。
