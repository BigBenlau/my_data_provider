# Step2：採集 → Tardis 正規化 → 對帳

三個階段各自執行。第 01 階段保存原生 channel，supervisor 只整合副本；不建立 L2、不計算 quotes、不合併 ticker／markPrice／OI。正式採集由使用者啟動。

| 階段 | 入口 | 輸出 |
|---|---|---|
| 01 原始採集 | `01_collect.py cluster` | `output/01_raw/<run>/<exchange>/` 的合併 JSONL；以 `export-csv` 按需產生原生 channel CSV.gz |
| 02 正規化 | `02_normalize_tardis_<交易所>.py` | `output/02_tardis/<run>/<交易所>/` |
| 03 對帳 | `03_compare_tardis.py` | `output/03_comparison/<run>/` |

第一次運行請看[五家交易所操作指南](RUN_01_02.md)。可選一家或多家做 30 秒短測、同時啟動長期採集，並逐家匯出原生 channel CSV。五家可分別執行第 02 階段產生 Tardis CSV。

## 啟動 Docker 採集

從 Git 建置映像、執行 30 秒短測、啟停正式採集及掛載資料卷的步驟見 [Docker 部署指南](DOCKER_DEPLOYMENT.md)。

本版五家短測、CSV 逐位元組驗證、封存與資源實測見[按需匯出驗收](evidence/ondemand_20261125T204054/README.md)。

## 設定與涵蓋範圍

```json
{
  "exchanges": {
    "binance": {
      "markets": {
        "binance": {"symbols": ["BTCUSDT"], "channels": ["trade", "depth@100ms", "depthSnapshot", "bookTicker"]},
        "binance-futures": {"symbols": ["BTCUSDT"], "channels": ["trade", "depth@0ms", "depthSnapshot", "bookTicker", "ticker", "markPrice@1s", "openInterest", "forceOrder"]}
      }
    }
  }
}
```

這是完整 `settings.json` 中的配置片段，其他連線、REST 與儲存設定仍保留。沒有 `exchanges` 的舊設定可以讀取；新版每個市場可以使用不同商品及 channel。

`01_collect.py cluster` 在每個 worker 內，依市場、原生 WS channel 及路由分組。每個 channel 為 BTC、ETH 各開專線，其餘商品共用一條 WS；只建立有訂閱的組。每組裝滿 `redundancy.group_size` 與交易所訂閱上限中較小的數量後，才開下一條。A、B 與換代候選使用相同分組。

例如預設 Binance 期貨 `bookTicker` 每個 worker 有三條 WS，分別訂閱 `BTCUSDT`、`ETHUSDT`、`GRTUSDT + SOLUSDT`。新增一般商品只需加入該市場的 `symbols`。以基礎資產判斷分組，例如 Upbit 的 `BTC-SOL` 屬於其餘商品。Binance、Bybit 若新增使用特殊報價幣的連寫商品，可在該市場明確指定 `"base_assets": {"BTCXYZ": "BTC"}`。同一基礎資產的不同報價交易對會進入同一資產組。

可用 `python 01_collect.py cluster --exchange binance --output /tmp/ws-layout --dry-run` 檢查 `groups` 中每條 WS 的 ID 及完整商品名單。運行日誌中的 `connect_attempt` 會記錄相同 ID，例如 `binance-futures/bookTicker/others/0`。改版部署或變更商品後使用新 run 目錄。

| 交易所主管 | 市場 ID | 預設商品 | 每商品原生 channel 數 |
|---|---|---|---|
| binance | binance、binance-futures | BTC、ETH、SOL、GRT 的 USDT 商品 | 現貨 4、期貨 8 |
| bybit | bybit-spot、bybit | BTC、ETH、SOL 的 USDT 商品 | 現貨 4、linear 5 |
| okx | okex、okex-swap | BTC、ETH、SOL 的 USDT／USDT-SWAP 商品 | 現貨 4、swap 7 |
| coinbase | coinbase | BTC-USD、ETH-USD、SOL-USD | 3 |
| upbit | upbit | KRW-BTC、KRW-ETH、KRW-SOL | 2 |

Binance 預設每天為 48 個原生 channel CSV；五家合計 123 個，包含只有 header 的靜默來源。這與第 02 階段的 32 組 Binance Tardis CSV 是不同輸出。

完整現有 channel 由各 adapter 的 `channels()` 宣告。JSON 尚未實作來源及歷史別名列於 [SOURCE_COVERAGE.md](SOURCE_COVERAGE.md)，本次不擴充那些來源。Binance 的公開來源不要求交易帳戶；本程式不使用私人交易端點或付費資料端點。

## Supervisor 與 worker 如何維護

```text
同一交易所主管（不同市場保留自己的商品、channel 與輸出位置）
  ├─ 相同 Worker A ─ 持久 spool ─┐
  ├─ 相同 Worker B ─ 持久 spool ─┼─ Unix socket → 去重／JSONL fsync → ACK
  └─ 相同 Worker C ─ 滾動期間 ──┘                            └─ 已封段 JSONL → 按需匯出 CSV.gz
```

- 正常兩個 worker；每 600 秒輪流嘗試替換一個，最多三個。新候選至少觀察 10 秒，每個 WS 來源至少三個共同原生事件；清算至少一個。所有必要來源都通過才讓舊 worker 停止收訊、排空並移除。成功完成後重新等待 600 秒，不追趕延期排程。
- 候選 120 秒未通過則放棄候選、保留兩條舊路。使用者已確認嚴格模式：啟動時列出沒有可靠事件身分的 channel，這類配置不建立注定失格的候選，保留現有兩路並定期記錄 rotation_blocked。有能力驗證的配置仍須每個來源共同事件通過；靜默清算或任何內容差異均不強行放行。
- worker 的待交付資料先進 SQLite WAL，只有持久批次才送出。supervisor 先交易保存去重決策／outbox／輸入游標，再寫入 JSONL 並 fsync，最後 ACK；確認後刪除 worker payload。崩潰邊界可截斷未提交尾端並重送。
- 主管失聯時現有 worker 繼續收訊、保存未確認資料、自行重連；不換代。主管恢復後接管同身分 worker。spool 有容量上限，滿載／磁碟錯誤停止並告警，不以丟掉最舊資料解決壅塞。
- 只有整條分組連線持續停滯、另一條有至少三筆新增事件，且控制狀態新鮮時，才修復該分組。單個冷門商品靜默不觸發整組重連。新 session 預設觀察 15 秒，兩次修復間隔至少 60 秒；指令綁定舊 session、接收進度及五秒有效期，恢復後自動失效。主動修復不降低已可用路由的評價。連線使用獨立 ping/pong；只有行情才能作為共同事件證據，ACK 或心跳不能替代。
- 跨路等待預設 5 秒，用於缺口／落後觀察。JSONL 按 supervisor 提交序號保存，不聲稱為交易所全局排序；遲到的新事件照樣保留。無歷史補數，較晚資料不會回溯改寫既有 quotes。

### 去重與歧義

以市場、商品、channel 隔離原生身分。Binance trade ID／book 更新序號、Bybit trade i／訂單簿事件、OKX tradeId／序號事件、Coinbase trade_id／ticker sequence、Upbit sequential_id 可辨識副本；重置、初始快照與增量另行區分。批次只逐個原生事件去重；部分重疊不刪除整批。拆分封裝帶 `batch_split_version=1`、`batch_index`、`batch_size`，原生事件欄位及外層必要資訊保留。

**沒有可靠原生身分的來源不強行去重。** 例如部分 ticker、清算及無序號快照，價量及時間相同仍可能是不同事件；會保留並標示 `identity_unverified=true`，日誌彙總 `ambiguity_preserved`。這些可能包含 A/B 副本，但不另存 A/B 資料集。相同強身分卻有不同內容時兩版都保留，標示 `content_conflict` 並阻止交接。不能把此策略宣稱為所有來源都已證明零重複或零缺失。

Binance depth 可核對 U/u 或 pu/u；OKX books 可核對 prevSeqId/seqId 及重置。其他來源只按 adapter 宣告觀察，不把 ID 遞增當成連續整數。已觀察的單路缺口由另一條序號鏈覆蓋時記錄 `single_path_gap_covered`；等待後仍未覆蓋記錄 `uncovered_sequence_gap`。兩路都換 session 仍保留合併序號監測。

## 檔案、持久化與重播

```text
<exchange-run>/
  manifest.json              # 封裝版本 3、來源配置、映像／程式／設定雜湊
  supervisor.log             # 五秒心跳、雙路覆蓋、交付、缺口、換代及封段游標
  status.json                # 最新程序／傳輸／來源與提交狀態
  raw/*.jsonl[.gz]           # 唯一正式原始資料；未封存段為 .jsonl.part
  _state/                    # SQLite 去重索引、提交游標、排程及控制 socket
  _spool/<worker>/            # 未確認交付暫存；ACK 清理 payload，退出排空後刪除目錄
```

CSV 仍一個原始 channel 一個檔案，按**接收時間 UTC 日期**分檔；第 02 階段才按事件日期輸出 Tardis。JSONL 包含原始 `raw`、`local_timestamp`、`source_session`、`delivery_sequence` 等追溯資訊；`seq` 是主管提交序號。原文價格／數量不經 float 轉換。

### 按需增量匯出壓縮 CSV

採集與停止排空都只寫原始資料。需要分析時，對每個交易所執行一次下列命令。同一個輸出目錄可以重複使用。若工作流程按日對帳，可在每日 UTC 00:05 執行同一命令；這是建議的呼叫時機，程式本身不建立排程。第 02 階段直接讀 JSONL，可略過原生 CSV 匯出。

```bash
python 01_collect.py export-csv --input "$DATA_ROOT/binance" \
  --output "$PWD/output/01_channels/run-001/binance"
```

輸出位於 `<output>/<market>/<symbol>/<接收 UTC 日期>/<channel>.csv.gz`。每次只讀開始時已封存的 JSONL，採集中與停止後均可執行；當前的 `.part` 留到下次。資料直接寫為 gzip，連今天的檔案也不必保留未壓縮副本。跨日遲到只在舊日期檔追加新的 gzip 成員，解壓後沿用原表頭與提交順序。首次遇到新日期會補齊靜默來源表頭。

`export.sqlite` 在指定輸出目錄中保存來源身分、提交序號與壓縮檔位元組偏移。每批最多 8 MiB 原始資料或 8,192 筆；單筆大型訊息完整處理。只同步有變更的檔案，完成後才更新游標。遇到中斷時對同一目錄重跑即可；工具會先撤回未提交的尾端。每個目的地只允許一個匯出程序。讀取 CSV 請等命令成功結束，並使用支援多成員 gzip 的讀取器，例如 Python 的 `gzip.open`。第一次使用請指定空目錄，舊的 `channels/` 保留。完整重建時另外指定新的輸出目錄。停止批次的唯讀備份只需 manifest 與完整封存 JSONL 即可匯出；接續採集仍需要完整狀態。

使用 Docker volume 時可開一次性的匯出程序。它只需以唯讀方式掛載來源與可寫的匯出目錄，無需行情網路或 Docker socket。

```bash
docker run --rm --network none -v "$COLLECTOR_VOLUME:/input:ro" \
  -v "$PWD/output/01_channels/run-002/binance:/export" market-collector:local \
  python /app/step2/01_collect.py export-csv --input /input --output /export
```

停止採集後可重播到新目錄，預設拒絕覆寫：

```bash
python 01_collect.py replay --input output/01_raw/run-001/binance \
  --output output/01_raw/replay-001
```

不正常結束先用相同版本啟動主管恢復；純離線恢復可用 `--recover`，只讀已提交偏移以內的完整資料。第 1 版 raw、第 2 版 gzip raw 讀取仍保留。`run`／`supervise` 是舊單程序相容命令，不具備雙副本整合；新部署使用 `cluster`。無 Docker 時可用 `cluster --backend local --duration 30` 作同入口的短測。

## 容量與運行日誌

`ws_coverage` 按市場、商品、WS channel 記錄目前兩名現役 worker 的 session。第一次收到資料、兩路皆收到資料與主管首次觀察到共同事件分別記為 `first_receive_us`、`both_receive_us`、`matched_observed_us`。共同事件依原生身分及已提交的雙路觀察辨識，普通來源需三筆、稀少清算一筆。缺少強身分時顯示 `observed_only`，換線後重新累積。共同事件是心跳抽樣證據，嚴格交接仍會另外查核。

每五秒 `[datetime] LEVEL ...` 日誌包含 worker 程序與 IPC、收訊來源數、入列／持久化／ACK 序號、spool bytes、最舊待交付年齡、記憶體佇列大小、ACK 往返時間、索引／落盤批次耗時、JSONL 提交與可供匯出的封段游標。最舊待交付超過十秒會告警。健康與資料使用獨立 IPC，資料 ACK 等待不會遮蔽收訊狀態；同步配額操作在背景執行緒序列化。`status.json` 保留逐來源時間。`observed_not_proven_complete` 表示可觀察的運行情況，並不是完整性證書。

REST 租約與配額跨 A/B/C 共用，換代不重置。Binance 以 exchangeInfo 與回應權重核對，保留設定較小的上限；檢查 REQUEST_WEIGHT、RAW_REQUESTS 的已公布視窗，處理 429／418、Retry-After 與冷卻。例行最多 70%，初始化／缺口快照 20%，剩下 10% 餘裕。OI 與快照分開公平派送，不重疊追趕。

例如 501 個期貨商品、設定每分鐘 1200 權重，OI 例行週期自動延長至 60 秒、深度快照至 1800 秒；初始化快照每分鐘最多 12 個，501 商品約需 42 個分鐘配額視窗；實際耗時仍受請求失敗及其他用量影響。source_warmup 逐市場／來源報告剩餘商品數及預估窗口，只有成功回應才完成初始化；失敗仍保留初始化優先級。不能同時要求 500+ 商品全部維持 6 秒 OI 又超過交易所容量。每個快照須在已訂閱 depth 並實際收訊後才取得；缺口只觸發該商品的新快照，不重新觸發全部商品。

配額只協調**這組部署**的副本。相同出口 IP 上其他程式或另一套採集部署也會消耗配額，須共同分配設定上限；回應 header 可反映部分外部用量。Docker 並不能把 IP 限額變成每個容器各一份。

依交易所各自限制分組及限制每秒連線嘗試，參考：[Bybit 連線規則](https://bybit-exchange.github.io/docs/v5/ws/connect)、[OKX 文件](https://www.okx.com/docs-v5/en/)、[Coinbase 連線規則](https://help.coinbase.com/en/exchange/managing-my-account/market-data-connections)、[Upbit 規則](https://docs.upbit.com/kr/reference/rate-limits)。OKX 優先使用現行 443 路由。Binance 不把 trade 換成 aggTrade，也不把 depth@0ms 換成 100ms；來源缺失如實記錄。

所有容器仍在同一主機。主機、共同網路、磁碟或交易所本身的資料限制可能同時影響 A/B；沒有跨主機容錯及歷史補數。每路 spool 預設上限 2 GiB，記憶體排隊上限 64 MiB；應以實際流量、磁碟速度與希望覆蓋的主管故障時間設定容量。

## 第 02／03 階段

```bash
python 02_normalize_tardis_binance.py --input output/01_raw/run-001/binance \
  --output output/02_tardis/run-001/binance
python 03_compare_tardis.py --help
```

入口的交易所名稱為 `binance`、`bybit`、`okx`、`coinbase`、`upbit`。每家使用各自的原生欄位、成交方向、事件時間及訂單簿協議。Binance 沿用 REST 快照與 WS 深度接合，序號缺口後等待新快照。Bybit、OKX 和 Coinbase 各自區分原生一檔行情與由 L2 推導的 quotes。Upbit 的 quotes 取自完整 orderbook 快照。按市場列出的資料類型、來源差異及 Docker 命令見[五家交易所操作指南](RUN_01_02.md)。

正規化報告記錄上游 commit、本地程式雜湊、行數及來源限制。欄位、快照、時間及去重差異見[原碼核對紀錄](reference/README.md)。

## 程式分工與測試

| 檔案 | 職責 |
|---|---|
| `01_collect.py` | 採集、按需原生 CSV 匯出、重播及舊命令相容 |
| `redundancy.py` | 交易所分組、配置、來源與版本身分 |
| `supervisor.py`、`container_runtime.py` | IPC 接管、容器管理與滾動換代 |
| `worker.py` | 所有 A/B/C 共用收訊、心跳與重送 |
| `delivery.py` | spool、JSONL 提交、SQLite 去重／恢復 |
| `replica_protocol.py`、`replica_monitor.py` | 原生事件拆分、身分、覆蓋與交接證據 |
| `rest_schedule.py` | 持久共用配額、輪詢、租約與路由 |
| `channel_export.py`、`raw.py` | 按需增量壓縮 CSV、原始格式與歷史重播 |
| `retention.py` | 無損壓縮、熱／歷史索引封存與崩潰邊界 |
| `collectors/*.py` | 五家交易所各自的協議規則 |
| `02_normalize_tardis_<交易所>.py`、`03_compare_tardis.py` | 獨立正規化與對帳 |

```bash
python -m unittest discover -s tests -v
python 01_collect.py cluster --exchange binance --output /tmp/unused --dry-run
```

固定測試涵蓋五家 ACK／心跳／原生身分、批次部分重疊、序號重置、雙路缺口、ACK 遺失重送、主管恢復、磁碟與容量故障、CSV 重播及 501 商品配額。每個自行維護函式最多 50 行，函式開頭與關鍵行使用繁體中文註解，亦有風格測試。

本次實際短測、重播、衝突及效能觀察見 [驗收紀錄](evidence/supervisor_20261124/README.md)。收訊時限不含停止後排空與 CSV 匯出時間；高流量時應預留足夠停止寬限。

## 運行問題修復與已確認政策（2026-11-24）

使用者已確認兩項政策：未驗證就不停止舊 worker；所有不同原文保留，只細分差異類別。`conflict_category` 區分 Binance spot depth 的發布時間差、Coinbase ticker 的滾動統計差與其他原生內容差異；分類不改變去重身分，也不放寬嚴格交接。無可靠身分的來源仍可能含副本，不宣稱已取得理想單路或證明零缺失。

```bash
# 不連交易所、不啟動 Docker；先檢查換代阻礙與設定配額下的初始化窗口。
python 01_collect.py cluster --exchange binance --output /tmp/collector-preflight --dry-run
```

主管獨立將封存 JSONL 以 gzip 無損壓縮；驗證解壓 SHA-256、同步檔案與目錄，再移除未壓縮副本。壓縮中斷保留原檔，離線 reader 不會重複讀兩個副本。第三版仍保留原始接收時間、行順序與 payload；第 02／03 階段介面不變。

`redundancy.index_hot_seconds` 預設 86400，已提交且最近一天沒有新觀察的索引搬到 `_state/index_archive.sqlite`。歷史身分及序號邊仍查詢得到，遲到事件不因 TTL 被漏判。新 run 的 `seen` 表將來源存為字典編號、可逆十六進位鍵存為二進位。其他原生字串保留原格式，熱區與歷史區共用永久字典。既有 run 使用原版本接續採集，不必就地遷移舊索引。封存分批以集合 SQL 處理，`index_archive_batch` 記錄包含等待提交鎖的耗時與每秒搬移筆數。每輪先持久複製、再刪熱索引，跨步驟崩潰可重做；不刪任何正式行情。`archive_batch_size=4096`、`archive_seconds=1` 限制每次工作量。歷史索引與正式行情仍隨保存期間增長；本次沒有設定資料保留期限或自動刪除歷史資料。磁碟需要按實際流量配置，封存速度不足也需調整批次與硬體容量。

部署新程式必須重新建置映像並使用新的 run 目錄。舊資料可離線重播；不可將新程式直接混入舊 run 的版本身分。實際 500+ 商品吞吐與 Ubuntu 主機的長期容量仍需另做壓測，排程模擬不代表該容量已通過。

效能調整仍保留 SQLite `synchronous=FULL`：WAL 自動 checkpoint 門檻改為 16384 頁（通常約 64 MiB），每個 SQLite 連線的頁面快取目標為 64 MiB，減少索引頁反覆寫入；快取按需使用，部署需計入全部連線的記憶體容量。每批最多 2048 筆／16 MiB，worker 每 0.25 秒刷出持久批次；ACK 僅計算已確認前綴的位元組，不掃描整個積壓。WAL 與歷史索引仍需磁碟預算，不能以關閉同步換取速度。暫時 TCP／TLS 失敗採 worker 自己的短期退避，不再全域封鎖另一條健康路由；確定的來源探測／路由協議失敗才更新共用診斷。退役 worker 的完整健康快照及連線標記會清理，交付游標與合併序號仍保留。

停止時 `status.json` 與 manifest 明確列出 `missing_required_sources`。必要來源缺失標為 `incomplete`；正常退出只代表程序已排空結束，不等於行情驗收通過。最新修復驗證見 [evidence/reliability_20261124/validation.json](evidence/reliability_20261124/validation.json)。

本次 86 項測試通過。隔離 Docker 原文重播使用相同 A/B worker、35 秒收訊時限，連同排空及 CSV 匯出約 45.3 秒完成：保存 94,922 列、必要來源齊全、四個清算來源無事件。來源中 94,319 個有可靠身分的 WS 事件版本全部保留；48 個 CSV 重新由 JSONL 生成後逐位元組一致，未確認交付與 outbox 均排空。無可靠身分的 572 筆仍保留，品質維持 `observed_not_proven_complete`，不是零缺失證明。

最新 volume 配置的首次 supervisor 觀察延遲 P95 約 4.32 秒，日誌採樣最舊待交付最高 3.98 秒；此指標不含最後 fsync。較早 bind mount 短測最高待交付約 27.62 秒，但兩次也有 SQLite 快取設定差異，不能把改善全部歸因於 volume。同原文的提交核心基準從約 15.00 秒降至 11.65 秒，輸出摘要相同；這不是全管線或正式主機的容量保證。

五家各約 30 秒真實連線嘗試皆因目前網路的 TLS EOF／連線重設而沒有行情，全部標為 `incomplete`。現有代理與容器直連均不可用，使用者已表示沒有替代代理；本次不更改系統網路、不開始正式採集。真實驗收與 500+ 商品長期吞吐仍未完成。兩項資料／換代政策已確認，沒有其他待確認決策。
