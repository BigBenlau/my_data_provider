# 02 離線重播與 Tardis 正規化

輸入是 01 的交易所批次根目錄，內有 `manifest.json` 和 JSONL 分段。整批重播使用已停止並排空的批次。指定 `--date` 時可讀取仍在採集的 supervisor 批次。選擇該家的 mapper 後，02 依提交順序重播、重建訂單簿與行情狀態，按事件時間的 UTC 日期寫出 Tardis CSV。可直接從 JSONL 開始，略過原生 channel CSV 匯出。[Step2 總覽](../README.md) · [01 採集](../01_collector/README.md)

## 執行正規化

從 Git 專案根目錄開啟 Bash，將 `RUN_ID` 和 `DATA_ROOT` 設為要處理的 01 批次。以下用主機 Python 單獨處理 Bybit。[Python 依賴](../requirements.txt)

```bash
cd tardis_sample/step2
export RUN_ID=run-001
export DATA_ROOT="$PWD/01_collector/output/raw/$RUN_ID"
EXCHANGE=bybit
python3 02_normalize_tardis/02_normalize_tardis.py --exchange "$EXCHANGE" \
  --input "$DATA_ROOT/$EXCHANGE" --output "$PWD/02_normalize_tardis/output/$RUN_ID/$EXCHANGE"
```

每家交易所的輸出目錄須尚未存在。同一份原文要再跑一次，請另選輸出目錄。若 01 啟用了冷盤，主機的 `COLD_ROOT` 須指向同一顆磁碟的掛載路徑，`--input` 仍指向熱盤的批次。[跨盤重播](../01_collector/DOCKER_DEPLOYMENT.md#選用第二顆本機磁碟)

### 多家並行與時間窗

從 `tardis_sample/step2` 執行下例，每家只重播一次。程式讀取 `$DATA_ROOT/<交易所>`，各自寫入 `$OUT_ROOT/<交易所>`。省略時間參數會重播所有已停止批次的資料。

```bash
OUT_ROOT="$PWD/02_normalize_tardis/output/${RUN_ID}-full"
python3 02_normalize_tardis/02_normalize_tardis.py \
  --exchange binance bybit okx coinbase upbit --jobs 2 \
  --input "$DATA_ROOT" --output "$OUT_ROOT"
```

若來源分散在多個目錄，可依 `--exchange` 的順序逐一傳入路徑。下例只寫出 2024-01-01 UTC 的事件。

```bash
python3 02_normalize_tardis/02_normalize_tardis.py \
  --exchange binance bybit --input "$DATA_ROOT/binance" "$DATA_ROOT/bybit" \
  --output "$OUT_ROOT-window/binance" "$OUT_ROOT-window/bybit" \
  --start-us 1704067200000000 --end-us 1704153600000000
```

`--jobs` 預設為 2，設為 1 可讓多家依序使用同一份記憶體預算。每家都有獨立程序、成交去重索引及報告。父程序在所有工作結束後輸出各家的狀態，若有工作失敗會傳回非零結束碼。

### 採集期間處理前一天

在 `tardis_sample/step2` 下執行。將 `RUN_ID` 換成實際批次並選擇交易所。下例會寫出前一個 **UTC 事件日** 的 Tardis CSV。

```bash
RUN_ID=run-001
EXCHANGE=binance
DAY=$(python3 -c 'from datetime import datetime, timedelta, timezone; print(datetime.now(timezone.utc).date() - timedelta(days=1))')
python3 02_normalize_tardis/02_normalize_tardis.py --exchange "$EXCHANGE" --date "$DAY" \
  --input "01_collector/output/raw/$RUN_ID/$EXCHANGE" \
  --output "02_normalize_tardis/output/$RUN_ID/$EXCHANGE/$DAY"
```

02 會在啟動時固定已封存的末筆 `seq`，記錄在 `normalization_report.json` 的 `inputs[].sealed_through_seq`。它從批次起點重播到該序號，使用午夜前的快照、深度及 ticker 狀態，只輸出指定事件日。耗時取決於要預熱的歷史長度。`--input` 請指定交易所批次根目錄，以便跨日期和熱冷盤讀取。01 原生 CSV 的分日依接收時間，因此午夜附近同一筆成交可落在不同的 01 和 02 日期。

稍後才封存、但事件時間屬於該日的行情，可在新輸出目錄重跑以納入。執行期間的即時批次支援已過去的 UTC 日期；若尚有該日期範圍的開放分段，等封段後再執行。`--date` 與 `--start-us`／`--end-us` 擇一使用。

### 使用 Docker 處理多家交易所

使用包含目前 02 程式的 `market-collector:local` 映像，將所有交易所的共同父目錄掛載一次。若已處理部分交易所，可只列出其餘待處理的名稱。[共用映像建置步驟](../01_collector/README.md#啟動採集)

```bash
OUT_ROOT="$PWD/02_normalize_tardis/output/${RUN_ID}-docker"
mkdir -p "$OUT_ROOT"
docker run --rm --network none \
  --mount "type=bind,source=$DATA_ROOT,target=/input,readonly" \
  --mount "type=bind,source=$OUT_ROOT,target=/output" \
  market-collector:local python /app/step2/02_normalize_tardis/02_normalize_tardis.py \
  --exchange binance bybit okx coinbase upbit --jobs 2 --input /input --output /output
```

各家分別使用 named volume 時，將各掛載點設為 `/input/binance`、`/input/bybit` 等，使各目錄都能找到對應的 manifest。原文跨熱冷盤時，在 `docker run` 加入 `--mount "type=bind,source=$COLD_ROOT,target=/cold,readonly" -e COLD_ROOT=/cold`。

## 各交易所的輸出

| `--exchange` | 資料類型 |
|---|---|
| `binance` | 現貨及期貨皆有 trades、quotes、book_ticker。期貨另有 derivative_ticker、liquidations。 |
| `bybit` | 現貨及線性永續皆有 trades、quotes、book_ticker。永續另有 derivative_ticker、liquidations。 |
| `okx` | 現貨及永續皆有 trades、quotes、book_ticker。永續另有 derivative_ticker。成交取自聚合 trades。現有永續來源缺少指數價與清算。 |
| `coinbase` | matches、level2_batch、ticker 分別產生 trades、quotes、book_ticker。level2_batch 的合批會影響可觀察的報價變更次數。 |
| `upbit` | trade 產生 trades，orderbook 的完整快照產生 quotes。 |

Binance 以 REST 深度快照銜接 WS 深度增量，遇到序號缺口後等待新快照。Bybit、OKX 和 Coinbase 各自區分原生最優買賣報價與由 L2 推導的 quotes。依預設四個商品採集 Binance 現貨及期貨，每個事件日期可對應 32 組 Tardis CSV。各家的原生欄位、成交方向、時間與快照規則見 [交易所 mapper](mappers/) 及 [上游原碼核對紀錄](reference/README.md)。

## 時間窗與復原

`--start-us` 包含起點，`--end-us` 排除終點，兩者皆為事件時間的 epoch 微秒，亦可只提供單邊。時間窗篩選寫出的列，完整輸入仍按提交順序讀取，以便預熱行情並處理遲到事件。

缺少 JSONL 分段、序號有洞、解壓失敗、壞行或缺少必要欄位時，程式會寫出 `event=normalization_warning` 並繼續讀可用資料。警告列出已知的 run、檔案、行號、欄位或首尾序號。未知範圍的缺口會清掉該 run 的簿與 ticker，之後的深度增量須重新接上快照；已見成交仍可跨缺口判重。全部警告保存在該交易所的 `normalization_report.json`，狀態為 `completed_with_warnings`。已停止但未正常封存的程序仍使用 `--recover`，主管批次的開放尾段依已提交末序號讀取。

### 長批次記憶體

JSONL 與 gzip JSONL 逐行解碼。每 `--batch-size` 筆（預設 4096）提交成交索引和逐檔統計，批次之間保留有效行情狀態。CSV 同時最多開啟 40 個檔案，最多快取 128 組市場、商品與日期。歷史日期統計、警告和批次清單暫存在磁碟，CLI 串流產生最後的報告。Python 呼叫 `replay()` 預設會載入完整回傳報告，需要精簡結果可用 `result='compact'`。

Binance 每個市場與商品最近 20,000 筆合併深度更新存於磁碟環形索引，快照接合時依序解碼所需原文。記憶體仍與同時工作的交易所、活躍商品及即時訂單簿深度有關。跨 run 清除行情狀態，每家程序退出時回收其記憶體。

## 輸出與品質報告

每家的輸出路徑為 `<output>/<市場>/<類型>/<UTC 日期>_<商品>.csv`。例如 Bybit 現貨成交位於 `bybit-spot/trades/2026-11-25_BTCUSDT.csv`。舊版的 `output/02_tardis/` 對應目前的 `02_normalize_tardis/output/`。

先查看 `<output>/normalization_report.json`。`status=complete` 表示重播完成。用 `rows`、`timestamp_ranges` 核對輸出筆數及時間範圍，再從 `inputs[].health`、`source_limitations`、`mapper_issues` 與 `recovery_warnings` 判斷來源缺失及復原狀況。`mapper_reference` 記錄上游 commit 與本地程式雜湊，供日後重現結果。

與 Tardis 參考資料比對時，將這家交易所的輸出根目錄傳給 03 的 `--ours`，並選擇雙方重疊的 UTC 時間窗。[03 對帳指令](../RUN_01_02.md#使用-03-對帳)
