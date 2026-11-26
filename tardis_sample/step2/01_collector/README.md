# 01 原始行情採集

每家交易所各有一個 supervisor 和兩個相同的 worker。supervisor 將兩路可辨識的副本整合為一份原生行情 JSONL。02 可以直接重播這份資料，原生 channel CSV 可按需匯出。[Step2 總覽](../README.md) · [02 正規化](../02_normalize_tardis/README.md)

## 啟動採集

從 Git 專案根目錄開啟 Bash，在同一個終端執行本頁命令。以下預設選擇全部五家。若只採集 OKX，可把陣列改成 `EXCHANGES=(okx)`。若採集兩家，可寫成 `EXCHANGES=(bybit upbit)`。商品與 channel 在建置前於設定檔中修改。[採集設定](settings.json)

```bash
cd tardis_sample/step2
EXCHANGES=(binance bybit okx coinbase upbit)
export RUN_ID=smoke-001
export DATA_ROOT="$PWD/01_collector/output/raw/$RUN_ID"
docker compose -f 01_collector/compose.yaml config --quiet
docker compose -f 01_collector/compose.yaml --profile build build collector
```

`collector` 是建置五家共用映像 `market-collector:local` 的目標，每個交易所服務以 `--exchange` 載入自己的採集模組。`RUN_ID` 是這批資料的名稱。每次做新的短測，可改用 `smoke-002` 等新名稱並重新設定 `DATA_ROOT`。

### 每家 30 秒的首次驗證

```bash
for exchange in "${EXCHANGES[@]}"; do
  docker compose -f 01_collector/compose.yaml run --rm "$exchange" \
    python /app/step2/01_collector/01_collect.py cluster --exchange "$exchange" \
    --output /data --host-output "$DATA_ROOT/$exchange" --duration 30 || break
done
```

這個迴圈逐家執行。每家收訊 30 秒後會停止、排空並封存，再開始下一家。請在每家的 `$DATA_ROOT/<服務名稱>/` 查看 `manifest.json` 的 `status` 是否為 `stopped`、`raw_records` 是否大於零。原始分段位於 `raw/<UTC 接收日期>/`。另看 `status.json` 的 `missing_required_sources` 與 `uncovered_gaps`，確認短測中觀察到的來源缺失及序號缺口。排空後可匯出原生 CSV，或直接將此目錄交給 02。

### 正式收集時同時啟動所選交易所

準備開始長期收集時，沿用上方的交易所清單，另外指定正式批次。

```bash
export RUN_ID=run-001
export DATA_ROOT="$PWD/01_collector/output/raw/$RUN_ID"
docker compose -f 01_collector/compose.yaml up -d "${EXCHANGES[@]}"
docker compose -f 01_collector/compose.yaml ps "${EXCHANGES[@]}"
docker compose -f 01_collector/compose.yaml logs --tail=30 "${EXCHANGES[@]}"
# 顯示 supervisor 動態建立的所有 worker，名稱含交易所、批次識別及 worker ID。
docker ps --filter label=collector.exchange --format '{{.Names}}'
```

到收集終點再執行停止命令。若另開終端，請先回到 `tardis_sample/step2` 並設回同一組 `EXCHANGES`、`RUN_ID` 和 `DATA_ROOT`。

```bash
docker compose -f 01_collector/compose.yaml stop "${EXCHANGES[@]}"
```

也可只停止其中一家，例如 `docker compose -f 01_collector/compose.yaml stop okx`。該家排空後即可處理其完整批次，其他服務可繼續採集。

## 監控採集

在同一個 Bash 終端選擇要看的交易所。持久日誌為 `$DATA_ROOT/<交易所>/supervisor.log`，每五秒記錄健康狀態。`status.json` 提供目前的 worker、WS 群組交接原因、積壓及未解缺口。

```bash
EXCHANGE=binance
tail -F "$DATA_ROOT/$EXCHANGE/supervisor.log"
```

可用 `docker compose -f 01_collector/compose.yaml logs -f "$EXCHANGE"` 查看 Compose 主管的終端日誌。主管升級交接、故障恢復、named volume 與第二顆磁碟的部署方式見 [Docker 部署指南](DOCKER_DEPLOYMENT.md)。

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

這是完整 `01_collector/settings.json` 中的配置片段，其他連線、REST 與儲存設定仍保留。沒有 `exchanges` 的舊設定可以讀取；新版每個市場可以使用不同商品及 channel。

`01_collector/01_collect.py cluster` 在每個 worker 內，依市場、原生 WS channel 及路由分組。每個 channel 為 BTC、ETH 各開專線，其餘商品共用一條 WS；只建立有訂閱的組。每組裝滿 `redundancy.group_size` 與交易所訂閱上限中較小的數量後，才開下一條。兩個 worker 及換線時的候選 WS 使用相同分組。

例如預設 Binance 期貨 `bookTicker` 每個 worker 有三條 WS，分別訂閱 `BTCUSDT`、`ETHUSDT`、`GRTUSDT + SOLUSDT`。新增一般商品只需加入該市場的 `symbols`。以基礎資產判斷分組，例如 Upbit 的 `BTC-SOL` 屬於其餘商品。Binance、Bybit 若新增使用特殊報價幣的連寫商品，可在該市場明確指定 `"base_assets": {"BTCXYZ": "BTC"}`。同一基礎資產的不同報價交易對會進入同一資產組。

可用 `python 01_collector/01_collect.py cluster --exchange binance --output /tmp/ws-layout --dry-run` 檢查 `groups` 中每條 WS 的 ID 及完整商品名單。運行日誌中的 `connect_attempt` 會記錄相同 ID，例如 `binance-futures/bookTicker/others/0`。改版部署或變更商品後使用新 run 目錄。

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
  ├─ Worker A 的 WS 群組 → 持久 spool ─┐
  └─ Worker B 的 WS 群組 → 持久 spool ─┴─ Unix socket → 去重 → JSONL fsync → ACK
          換線期間其中一個群組加開一條 WS                  └─ 封段 → 按需匯出 channel CSV
```

- 每家主管管理兩個相同 worker。例行排程每 600 秒在 A/B 之間輪流選一個群組，成功後再等 600 秒。先訂閱候選 WS，至少觀察新 session 10 秒，逐商品核對三條線已提交的共同事件。一般來源需三筆，稀少來源需一筆。通過才關舊 WS。
- 弱身分來源保留兩條線。達到 `ws_max_age`（預設 82,800 秒）會優先安排候選。未能驗證時撤回候選並記錄原因。若多組同時到期，依單一交接名額排隊。
- 控制通道證實一條線停滯、對側持續收到行情時，立即嘗試候選並與健康側核對。現役 session 若出現原生序號缺口或無效行情，也會優先嘗試該群組。實際網路斷線由該路自行重連。積壓時暫停例行換線，縮小索引整理批次；三個新鮮低水位心跳後恢復。
- worker 的待交付資料先進 SQLite WAL，只有持久批次才送出。supervisor 先交易保存去重決策／outbox／輸入游標，再寫入 JSONL 並 fsync，最後 ACK；確認後刪除 worker payload。崩潰邊界可截斷未提交尾端並重送。
- 主管失聯時現有 worker 繼續收訊、保存未確認資料、自行重連；不換代。主管恢復後接管同身分 worker。spool 有容量上限，滿載／磁碟錯誤停止並告警，不以丟掉最舊資料解決壅塞。
- 判斷收訊停滯時，主管要求整條分組連線持續停滯、另一條有至少三筆新增事件，且控制狀態新鮮。單個冷門商品靜默不觸發整組重連。新 session 預設觀察 15 秒，兩次修復間隔至少 60 秒；修復請求綁定舊 session、接收進度及五秒有效期。主動修復不降低已可用路由的評價。連線使用獨立 ping/pong；只有行情才能作為共同事件證據，ACK 或心跳不能替代。
- 跨路等待預設 5 秒，用於缺口／落後觀察。JSONL 按 supervisor 提交序號保存，不聲稱為交易所全局排序；遲到的新事件照樣保留。無歷史補數，較晚資料不會回溯改寫既有 quotes。

### 去重與歧義

以市場、商品、channel 隔離原生身分。Binance trade ID／book 更新序號、Bybit trade i／訂單簿事件、OKX tradeId／序號事件、Coinbase trade_id／ticker sequence、Upbit sequential_id 可辨識副本；重置、初始快照與增量另行區分。批次只逐個原生事件去重；部分重疊不刪除整批。拆分封裝帶 `batch_split_version=1`、`batch_index`、`batch_size`，原生事件欄位及外層必要資訊保留。

**沒有可靠原生身分的來源不強行去重。** 例如部分 ticker、清算及無序號快照，價量及時間相同仍可能是不同事件；會保留並標示 `identity_unverified=true`，日誌彙總 `ambiguity_preserved`。這些可能包含 A/B 副本，但不另存 A/B 資料集。相同強身分卻有不同內容時兩版都保留，標示 `content_conflict` 並阻止交接。不能把此策略宣稱為所有來源都已證明零重複或零缺失。

Binance depth 可核對 U/u 或 pu/u；OKX books 可核對 prevSeqId/seqId 及重置。其他來源只按 adapter 宣告觀察，不把 ID 遞增當成連續整數。已觀察的單路缺口由另一條序號鏈覆蓋時記錄 `single_path_gap_covered`；等待後仍未覆蓋記錄 `uncovered_sequence_gap`。兩路都換 session 仍保留合併序號監測。

新 run 的 `manifest.json` 記錄 `dedup_rule_version=2`。強身分事件比較完整原生封裝，批次只排除其他成員。Bybit 的同一成交若只有外層 `ts` 不同，01 保留兩份並記錄發布欄位差異，02 依成交時間、方向、價、量及 ID 去重。舊批次仍可離線重播。

正常逐筆收訊證據滾動保留 24 小時，以整數索引記錄副本與 session。到期後按 UTC 小時、來源及路徑彙總。交接、衝突、未解序號缺口與待交付範圍可延後整理。歷史查重持續使用完整身分及內容指紋。第二顆本機磁碟的掛載、健康指標和日誌指令見 [Docker 部署文件](DOCKER_DEPLOYMENT.md#監控群組換線及資料盤)。

## 原始批次目錄

```text
<exchange-run>/
  manifest.json              # 封裝版本 3、來源配置、映像／程式／設定雜湊
  supervisor.log             # 五秒心跳、雙路覆蓋、交付、缺口、換代及封段游標
  status.json                # 最新程序／傳輸／來源與提交狀態
  raw/YYYY-MM-DD/*.jsonl[.gz] # 該 UTC 接收日的原始分段，正在寫入的段為 .jsonl.part
  _state/                    # SQLite 去重索引、提交游標、排程及控制 socket
  _spool/<worker>/            # 未確認交付暫存；ACK 清理 payload，退出排空後刪除目錄
```

CSV 仍一個原始 channel 一個檔案，按**接收時間 UTC 日期**分檔；第 02 階段才按事件日期輸出 Tardis。JSONL 包含原始 `raw`、`local_timestamp`、`source_session`、`delivery_sequence` 等追溯資訊；`seq` 是主管提交序號。原文價格／數量不經 float 轉換。

## 隔天取出前一天的資料

01 以 worker 的 `local_timestamp`（UTC 接收時間）決定日期。UTC 00:00 對應上海時間 08:00。即使同一個提交批次跨午夜，每個 JSONL 分段也只屬於一個日期。worker 稍後補交的舊行情會進入原接收日的目錄。每日目錄保留短分段，讓封存、冷盤搬移及故障恢復能逐段進行。整批重播依檔名中的首筆 `seq` 排序。

從 `tardis_sample/step2` 執行以下指令。將 `RUN_ID` 換成正在採集的批次；可用 `binance`、`bybit`、`okx`、`coinbase` 或 `upbit` 作為 `EXCHANGE`。

```bash
RUN_ID=run-001
EXCHANGE=binance
DAY=$(python3 -c 'from datetime import datetime, timedelta, timezone; print(datetime.now(timezone.utc).date() - timedelta(days=1))')
RAW="01_collector/output/raw/$RUN_ID/$EXCHANGE"

# 需要攜帶整天原文時，匯出一個 <DAY>.jsonl.gz。
python3 01_collector/01_collect.py export-jsonl --input "$RAW" --date "$DAY" \
  --output "01_collector/output/daily-jsonl/$RUN_ID/$EXCHANGE"

# 每個市場、商品、原生 channel 在這一天各有一個 CSV.gz。
python3 01_collector/01_collect.py export-csv --input "$RAW" --date "$DAY" \
  --output "01_collector/output/channels/$RUN_ID/$EXCHANGE"
```

兩個命令都可在 supervisor 繼續採集時執行。`export-jsonl` 逐行保留原文，回報 `sealed_through_seq` 和筆數。目的地已有同名檔時會拒絕覆寫；若有遲交且要取得更新的整日版本，可指定新的匯出目錄。`export-csv` 則可對同一目錄重跑，補上已封存的新列；同一批次的逐日和全量匯出可共用該目錄。回報中的 `rows` 為該目錄累計匯出的資料列數。若目標日期的尾段仍在寫入，請在下一次封段後重試。第 02 階段直接使用原始批次根目錄進行跨日預熱。[02 每日正規化](../02_normalize_tardis/README.md#採集期間處理前一天)

## 匯出原生 channel CSV

沿用要匯出的那一批 `RUN_ID` 和 `DATA_ROOT`。以下每次掛載一家交易所的輸入，並將各家的匯出游標分開保存。採集中可匯出已封存的 JSONL，排空後再執行同一命令即可補齊尾段。

```bash
mkdir -p "$PWD/01_collector/output/channels/$RUN_ID"
for exchange in "${EXCHANGES[@]}"; do
  docker run --rm --network none \
    --mount "type=bind,source=$DATA_ROOT/$exchange,target=/input,readonly" \
    --mount "type=bind,source=$PWD/01_collector/output/channels/$RUN_ID,target=/output" \
    market-collector:local python /app/step2/01_collector/01_collect.py export-csv \
    --input /input --output "/output/$exchange" || break
done
```

輸出依交易所、市場、商品、接收時間的 UTC 日期及原生 channel 存放。例如 OKX 現貨成交位於 `01_collector/output/channels/<RUN_ID>/okx/okex/BTC-USDT/<日期>/trades.csv.gz`，Upbit 則是 `upbit/upbit/KRW-BTC/<日期>/trade.csv.gz`。直接使用主機 Python 時，可執行同一入口，例如 `python3 01_collector/01_collect.py export-csv --input "$DATA_ROOT/bybit" --output "$PWD/01_collector/output/channels/$RUN_ID/bybit"`。[Python 依賴](../requirements.txt)

輸出位於 `<output>/<market>/<symbol>/<接收 UTC 日期>/<channel>.csv.gz`。每次只讀開始時已封存的 JSONL，採集中與停止後均可執行；當前的 `.part` 留到下次。資料直接寫為 gzip，連今天的檔案也不必保留未壓縮副本。跨日遲到只在舊日期檔追加新的 gzip 成員，解壓後沿用原表頭與提交順序。首次遇到新日期會補齊靜默來源表頭。

`export.sqlite` 在指定輸出目錄中保存來源身分、提交序號與壓縮檔位元組偏移。每批最多 8 MiB 原始資料或 8,192 筆；單筆大型訊息完整處理。只同步有變更的檔案，完成後才更新游標。遇到中斷時對同一目錄重跑即可；工具會先撤回未提交的尾端。每個目的地只允許一個匯出程序。讀取 CSV 請等命令成功結束，並使用支援多成員 gzip 的讀取器，例如 Python 的 `gzip.open`。第一次使用請指定空目錄，舊的 `channels/` 保留。完整重建時另外指定新的輸出目錄。停止批次的唯讀備份只需 manifest 與完整封存 JSONL 即可匯出；接續採集仍需要完整狀態。

使用 Docker named volume 時，將輸入掛載改為 `--mount "type=volume,source=$COLLECTOR_VOLUME,target=/input,readonly"`，逐家指定該批次的 volume。若有分段已移到冷盤，另外唯讀掛載原冷盤並設定容器內的 `COLD_ROOT`。[冷盤掛載與重播](DOCKER_DEPLOYMENT.md#選用第二顆本機磁碟)

## 重播及歷史批次

要從已停止採集的批次重建原生 channel CSV，請指定新的輸出目錄。若需每日對帳，可用外部排程在 UTC 00:05 匯出前一天已封存的分段。

```bash
python 01_collector/01_collect.py replay --input 01_collector/output/raw/run-001/binance \
  --output 01_collector/output/channels/replay-001
```

不正常結束先用相同版本啟動主管恢復；純離線恢復可用 `--recover`，只讀已提交偏移以內的完整資料。第 1 版 raw、第 2 版 gzip raw 讀取仍保留。`run`／`supervise` 是舊單程序相容命令，不具備雙副本整合；新部署使用 `cluster`。無 Docker 時可用 `cluster --backend local --duration 30` 作同入口的短測。

舊路徑 `output/01_raw/`、`output/01_channels/` 分別對應 `01_collector/output/raw/`、`01_collector/output/channels/`。外部腳本與 systemd 設定請使用新入口。搬移歷史批次後，將實際 run 路徑傳給 `--input`。歷史 manifest、日誌和報告保留採集時的路徑。

## 容量與運行日誌

`ws_coverage` 按市場、商品、WS channel 記錄目前兩名現役 worker 的 session。第一次收到資料、兩路皆收到資料與主管首次觀察到共同事件分別記為 `first_receive_us`、`both_receive_us`、`matched_observed_us`。共同事件依原生身分及已提交的雙路觀察辨識，普通來源需三筆、稀少清算一筆。缺少強身分時顯示 `observed_only`，換線後重新累積。共同事件是心跳抽樣證據，嚴格交接仍會另外查核。

每五秒 `[datetime] LEVEL ...` 日誌包含 worker 程序與 IPC、收訊來源數、入列／持久化／ACK 序號、spool bytes、最舊待交付年齡、記憶體佇列大小、ACK 往返時間、索引／落盤批次耗時、JSONL 提交與可供匯出的封段游標。最舊待交付超過十秒會告警。健康與資料使用獨立 IPC，資料 ACK 等待不會遮蔽收訊狀態；同步配額操作在背景執行緒序列化。`status.json` 保留逐來源時間。`observed_not_proven_complete` 表示可觀察的運行情況，並不是完整性證書。

兩個 worker 共用 REST 租約與配額，換線時沿用同一份排程。Binance 以 exchangeInfo 與回應權重核對，保留設定較小的上限；檢查 REQUEST_WEIGHT、RAW_REQUESTS 的已公布視窗，處理 429／418、Retry-After 與冷卻。例行最多 70%，初始化／缺口快照 20%，剩下 10% 餘裕。OI 與快照分開公平派送，不重疊追趕。

例如 501 個期貨商品、設定每分鐘 1200 權重，OI 例行週期自動延長至 60 秒、深度快照至 1800 秒；初始化快照每分鐘最多 12 個，501 商品約需 42 個分鐘配額視窗；實際耗時仍受請求失敗及其他用量影響。source_warmup 逐市場／來源報告剩餘商品數及預估窗口，只有成功回應才完成初始化；失敗仍保留初始化優先級。不能同時要求 500+ 商品全部維持 6 秒 OI 又超過交易所容量。每個快照須在已訂閱 depth 並實際收訊後才取得；缺口只觸發該商品的新快照，不重新觸發全部商品。

配額只協調**這組部署**的副本。相同出口 IP 上其他程式或另一套採集部署也會消耗配額，須共同分配設定上限；回應 header 可反映部分外部用量。Docker 並不能把 IP 限額變成每個容器各一份。

依交易所各自限制分組及限制每秒連線嘗試，參考：[Bybit 連線規則](https://bybit-exchange.github.io/docs/v5/ws/connect)、[OKX 文件](https://www.okx.com/docs-v5/en/)、[Coinbase 連線規則](https://help.coinbase.com/en/exchange/managing-my-account/market-data-connections)、[Upbit 規則](https://docs.upbit.com/kr/reference/rate-limits)。OKX 優先使用現行 443 路由。Binance 不把 trade 換成 aggTrade，也不把 depth@0ms 換成 100ms；來源缺失如實記錄。

所有容器仍在同一主機。主機、共同網路、磁碟或交易所本身的資料限制可能同時影響 A/B；沒有跨主機容錯及歷史補數。每路 spool 預設上限 2 GiB，記憶體排隊上限 64 MiB；應以實際流量、磁碟速度與希望覆蓋的主管故障時間設定容量。

## 程式分工與測試

| 檔案 | 職責 |
|---|---|
| `01_collector/01_collect.py` | 採集、按需原生 CSV 匯出及重播 |
| `redundancy.py` | 交易所分組、配置、來源與版本身分 |
| `supervisor.py`、`container_runtime.py`、`ws_rotation.py` | IPC 接管、容器管理與 WS 群組換線 |
| `worker.py` | 兩個 worker 共用收訊、連線層心跳與重送。訂閱含 REST 來源時才建立 REST 連線池、啟動領取租約的輪詢任務 |
| `delivery.py` | spool、JSONL 提交、SQLite 去重／恢復 |
| `replica_protocol.py`、`replica_monitor.py` | 原生事件拆分、身分、覆蓋與交接證據 |
| `rest_schedule.py` | 持久共用配額、輪詢、租約與路由 |
| `01_collector/channel_export.py`、`01_collector/raw.py` | 按需增量壓縮 CSV、原始資料寫入與匯出 |
| `common/raw_reader.py`、`common/protocol.py` | 三版原始紀錄讀取、JSON 解碼與市場對應表 |
| `common/<交易所>.py` | 採集和離線重播共用的原生訊息分類及欄位辨識 |
| `retention.py`、`cold.py` | 無損壓縮、熱／歷史索引封存與崩潰邊界 |
| `01_collector/collectors/<交易所>.py` | 各家的預設商品、訂閱、應用層心跳、事件身分、批次拆分與差異判定。OKX 的純文字心跳在 `okx.py`，Binance 的 REST 配額與原生 CSV 欄位在 `binance.py` |

```bash
python3 -m unittest discover -s tests -v
```

固定測試涵蓋五家 ACK／心跳／原生身分、批次部分重疊、序號重置、雙路缺口、ACK 遺失重送、主管恢復、磁碟與容量故障、CSV 重播及 501 商品配額。每個自行維護函式最多 50 行，函式開頭與關鍵行使用繁體中文註解，亦有風格測試。

收訊時限只計算採集時間。高流量批次還需要時間排空並匯出 CSV。

## 2026-11-26 短測與儲存驗收

五家同時使用正式群組與完整商品設定，各採集 30 秒並排空。Binance 15,683 筆、Bybit 8,067 筆、OKX 3,837 筆、Coinbase 2,985 筆、Upbit 692 筆。Binance 現貨 GRT 的 `bookTicker` 和 `trade` 在這個視窗缺少事件，該批次標為 `incomplete`。123 個原生 channel CSV 分別匯出兩次，壓縮檔位元組一致，五家的 02 離線重播均完成。

同一組 8,192 筆固定成交由 A/B 分別提交，每批 256 筆，與 Git `7994399dcb37` 交替各跑三輪。以 SQLite `dbstat` 計算觀察表、路徑字典及相關索引，共從 2,064,384 降到 1,314,816 位元組（36.3%）。從 supervisor 接收批次到可回 ACK 的 P95，三輪中位數由 13.0 增為 16.6 毫秒。這次短測同時觀察到節省空間與提交延遲增加。24 小時邊界由固定時間測試覆蓋，跨裝置測試使用 Docker 中分別掛載的兩個檔案系統。

## 2026-11-24 歷史驗收與調整

目前的群組交接須通過驗證才能停止舊 WS。同身分的不同原文保留並記錄差異類別。`conflict_category` 區分 Binance spot depth 的發布時間差、Coinbase ticker 的滾動統計差與其他原生內容差異；分類不改變去重身分，也不放寬嚴格交接。無可靠身分的來源仍可能含副本，不宣稱已取得理想單路或證明零缺失。

主管獨立將封存 JSONL 以 gzip 無損壓縮；驗證解壓 SHA-256、同步檔案與目錄，再移除未壓縮副本。壓縮中斷保留原檔，離線 reader 不會重複讀兩個副本。第三版仍保留原始接收時間、行順序與 payload；第 02／03 階段介面不變。

`redundancy.index_hot_seconds` 預設 86400，已提交且最近一天沒有新觀察的索引搬到 `_state/index_archive.sqlite`。歷史身分及序號邊仍查詢得到，遲到事件不因 TTL 被漏判。新 run 的 `seen` 表將來源存為字典編號、可逆十六進位鍵存為二進位。其他原生字串保留原格式，熱區與歷史區共用永久字典。既有 run 使用原版本接續採集，不必就地遷移舊索引。封存分批以集合 SQL 處理，`index_archive_batch` 記錄包含等待提交鎖的耗時與每秒搬移筆數。每輪先持久複製、再刪熱索引，跨步驟崩潰可重做；不刪任何正式行情。`archive_batch_size=4096`、`archive_seconds=1` 限制每次工作量。歷史索引與正式行情仍隨保存期間增長；本次沒有設定資料保留期限或自動刪除歷史資料。磁碟需要按實際流量配置，封存速度不足也需調整批次與硬體容量。

部署新程式必須重新建置映像並使用新的 run 目錄。舊資料可離線重播；不可將新程式直接混入舊 run 的版本身分。實際 500+ 商品吞吐與 Ubuntu 主機的長期容量仍需另做壓測，排程模擬不代表該容量已通過。

效能調整仍保留 SQLite `synchronous=FULL`：WAL 自動 checkpoint 門檻改為 16384 頁（通常約 64 MiB），每個 SQLite 連線的頁面快取目標為 64 MiB，減少索引頁反覆寫入；快取按需使用，部署需計入全部連線的記憶體容量。每批最多 2048 筆／16 MiB，worker 每 0.25 秒刷出持久批次；ACK 僅計算已確認前綴的位元組，不掃描整個積壓。WAL 與歷史索引仍需磁碟預算，不能以關閉同步換取速度。暫時 TCP／TLS 失敗採 worker 自己的短期退避，不再全域封鎖另一條健康路由；確定的來源探測／路由協議失敗才更新共用診斷。退役 worker 的完整健康快照及連線標記會清理，交付游標與合併序號仍保留。

停止時 `status.json` 與 manifest 明確列出 `missing_required_sources`。必要來源缺失標為 `incomplete`；正常退出只代表程序已排空結束，不等於行情驗收通過。

當時 86 項測試通過。隔離 Docker 原文重播使用相同 A/B worker、35 秒收訊時限，連同排空及 CSV 匯出約 45.3 秒完成：保存 94,922 列、必要來源齊全、四個清算來源無事件。來源中 94,319 個有可靠身分的 WS 事件版本全部保留；48 個 CSV 重新由 JSONL 生成後逐位元組一致，未確認交付與 outbox 均排空。無可靠身分的 572 筆仍保留，品質維持 `observed_not_proven_complete`，不是零缺失證明。

當時的 volume 配置的首次 supervisor 觀察延遲 P95 約 4.32 秒，日誌採樣最舊待交付最高 3.98 秒；此指標不含最後 fsync。較早 bind mount 短測最高待交付約 27.62 秒，但兩次也有 SQLite 快取設定差異，不能把改善全部歸因於 volume。同原文的提交核心基準從約 15.00 秒降至 11.65 秒，輸出摘要相同；這不是全管線或正式主機的容量保證。

當日五家各約 30 秒的連線嘗試皆遇到 TLS EOF 或連線重設，批次均標為 `incomplete`。
