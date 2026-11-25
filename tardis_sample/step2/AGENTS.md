# Step2 維護規則

本檔適用於 `tardis_sample/step2/` 及其子目錄。

## 三階段邊界

- `01_collect.py` 與 `collectors/` 只負責原始採集、原文保存、來源識別、連線管理、REST 排程及採集日誌。
- 採集端不得建立或合併 L2 訂單簿、計算 quotes、合併 ticker／markPrice／OI、過濾成交類別或進行行情語義去重。可觀察更新序號並記錄缺口、要求補取新快照，但不得改寫原文或宣稱訂單簿已同步。
- `02_normalize_tardis.py` 才負責訂單簿重建、欄位映射、狀態合併與正規化去重；只讀已保存的原始資料。
- `03_compare_tardis.py` 只負責與參考資料對帳。三階段程式名稱、輸出目錄及執行方式必須分開。

## Supervisor 副本整合例外

使用者已授權第 01 階段的 supervisor 消除可辨識的同一原生事件副本、拆分部分重疊批次，以及保存缺口與內容衝突。這不授權建立 L2、過濾成交類別或合併 ticker／markPrice／OI。只有 supervisor 的 JSONL 是長期基準；worker 未確認緩衝必須保留，確認後清理。無可靠序號的來源不得宣稱已證明零缺失。

## 每次修改正規化前，必須重新核對 GitHub

每次修改 `02_normalize_tardis.py`，或修改會影響其輸出的共用函式、欄位、時間、排序、去重、快照及狀態規則，都必須在當次任務重新連線讀取下列 GitHub 原碼，不能只依賴記憶、舊摘要或本地 `reference/`：

1. Binance mapper：<https://github.com/tardis-dev/tardis-node/blob/master/src/mappers/binance.ts>
2. ticker 狀態 helper：<https://github.com/tardis-dev/tardis-node/blob/master/src/mappers/mapper.ts>
3. 涉及 quotes 時，另讀 <https://github.com/tardis-dev/tardis-node/blob/master/src/computable/booksnapshot.ts> 及 <https://github.com/tardis-dev/tardis-node/blob/master/src/orderbook.ts>。

執行要求：

- 解析當次上游 commit SHA，使用同一 SHA 的原始檔核對，避免混合不同版本。
- 明確比較本次變更涉及的欄位、事件時間、接收時間、空值、輸出觸發、快照緩存及重置行為。
- 在 `reference/README.md` 記錄核對日期、完整 commit、永久 GitHub 連結、原碼 SHA-256、對應本地程式與有意保留的差異；第三方參考碼保留原文及授權。
- 正規化報告必須記錄所用上游版本及本地程式雜湊，讓原始資料可依版本重播。執行離線正規化本身不需要連線 GitHub。
- 修改後執行涵蓋受影響語義的固定資料回歸測試；不可因 CSV 欄位相同，就宣稱與 Tardis 逐列完全一致。
- 最終回覆指出本次核對的 GitHub 連結、commit、修改內容、測試及仍存在的差異。
- 若當次無法讀取 GitHub，可完成不影響正規化的工作；不得把未核對的變更描述為已符合上游。

## 程式與驗證風格

- 自行維護的每個 Python 函式最多 50 行，計入裝飾器、註解及空白行。
- 函式開頭及關鍵步驟使用繁體中文 `#` 註解；第三方原碼不翻譯、不自行改寫。
- 使用 Python `logging`，格式為 `[datetime] LEVEL ...`；採集需保留定期心跳及異常日誌。
- 保留既有 raw／CSV，不覆寫使用者採集成果。修改後真實採集只做約 30 秒短測；正式長期採集由使用者啟動。
