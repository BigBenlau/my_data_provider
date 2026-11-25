# Tardis 原碼核對紀錄

## 2026-11-26 目錄重排核對

重新從 GitHub 讀取 master 的 commit `a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73`。下列所有檔案與搬移前的工作區版本逐位元組相同。

| 上游原始檔 | SHA-256 |
|---|---|
| [binance.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/binance.ts) | `819260e0bee154a61b8dcbfb448bfa65276c14db4b57999d69528ba7daa6bf9e` |
| [bybit.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/bybit.ts) | `1c2f4d1aace0bc22b4d5203031eb59c1d159596b269bbcf3eedc87ca0fa7617f` |
| [okex.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/okex.ts) | `89cca5ab9fb87fed5238a83fe0f7777678c26e424969813a6204a1ffbc6432d0` |
| [coinbase.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/coinbase.ts) | `a1ca942e3e1a50279f11221d931e14d412c234e3fc5c5b45d61698b790142edf` |
| [upbit.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/upbit.ts) | `42e41efa1a6a426d0974c5205d669d1b6a3f52f0f693e9bc89dc62424c2212ac` |
| [mapper.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/mapper.ts) | `15221302ca9a469b2c6aa8c3d51662dafb44c7644f452da340046eefb0559f62` |
| [booksnapshot.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/computable/booksnapshot.ts) | `ff176e69039a3c9d85de13973013a677e8e189fe892d4a07df341ed9d07770a1` |
| [orderbook.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/orderbook.ts) | `960febd312668d6a43ec38d95d2c15abb9bd374338dadf8dbaae8701bd29b576` |
| [handy.ts](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/handy.ts) | `17c69e74ccdd15dc95495cab0c3dc50fa74b80c51837d84ada2017aadf855d4a` |
| [LICENSE](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/LICENSE) | `1f256ecad192880510e84ad60474eab7589218784b9a50bc7ceee34c2b91f1d5` |

五家 mapper 位於 `../mappers/`，統一由 `--exchange` 選擇。行情欄位、事件時間、接收時間、空值、輸出觸發、快照緩存與狀態重置依搬移前的規則。共用訊息分類及 raw 讀取移至 `../../common/`。版號和實作雜湊路徑由報告保存。

固定輸入下，五家共 19 份 CSV 的位元組、輸出行數、時間範圍和異常計數一致。五份對帳報告也逐位元組一致。本地的序號檢查、候選快照、輸出精度及來源差異如以下歷史紀錄。

## 2026-11-26 交易所採集模組拆分

此次再次核對上述固定 commit `a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73` 的九個 TypeScript 檔案，SHA-256 均符合表中紀錄。原生訊息分類移至 `../../common/<交易所>.py`，採集和離線正規化共用同一份分類函式。此次搬移沿用原有欄位、原生時間、接收時間、空值及觸發條件，快照銜接與狀態重置仍由原有 mapper 處理。本地嚴格序號檢查及接收時間與 Tardis 的差異見下列紀錄。

## 歷史核對紀錄

## 五家交易所的離線正規化

2026-11-25（Asia/Shanghai）從 GitHub 解析 master 並讀取同一 commit 的原碼。固定版本為 `a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73`。Binance 與先前保存的四個參考檔逐位元組相同。第三方 TypeScript 依原文保存。[上游目錄](https://github.com/tardis-dev/tardis-node/tree/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers) · [授權](LICENSE)

| 本地檔案 | GitHub | SHA-256 |
|---|---|---|
| [binance.ts](binance.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/binance.ts) | `819260e0bee154a61b8dcbfb448bfa65276c14db4b57999d69528ba7daa6bf9e` |
| [bybit.ts](bybit.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/bybit.ts) | `1c2f4d1aace0bc22b4d5203031eb59c1d159596b269bbcf3eedc87ca0fa7617f` |
| [okex.ts](okex.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/okex.ts) | `89cca5ab9fb87fed5238a83fe0f7777678c26e424969813a6204a1ffbc6432d0` |
| [coinbase.ts](coinbase.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/coinbase.ts) | `a1ca942e3e1a50279f11221d931e14d412c234e3fc5c5b45d61698b790142edf` |
| [upbit.ts](upbit.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/upbit.ts) | `42e41efa1a6a426d0974c5205d669d1b6a3f52f0f693e9bc89dc62424c2212ac` |
| [mapper.ts](mapper.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/mappers/mapper.ts) | `15221302ca9a469b2c6aa8c3d51662dafb44c7644f452da340046eefb0559f62` |
| [booksnapshot.ts](booksnapshot.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/computable/booksnapshot.ts) | `ff176e69039a3c9d85de13973013a677e8e189fe892d4a07df341ed9d07770a1` |
| [orderbook.ts](orderbook.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/orderbook.ts) | `960febd312668d6a43ec38d95d2c15abb9bd374338dadf8dbaae8701bd29b576` |
| [handy.ts](handy.ts) | [固定原碼](https://github.com/tardis-dev/tardis-node/blob/a6b0cfa0848fa2a8b0b46a10b25a1427ce944c73/src/handy.ts) | `17c69e74ccdd15dc95495cab0c3dc50fa74b80c51837d84ada2017aadf855d4a` |

| 第 02 階段入口 | 核對的來源、時間、方向與輸出條件 |
|---|---|
| `02_normalize_tardis_binance.py` | 現貨與 USD-M 沿用既有成交過濾、深度快照接合、原生 bookTicker 與 ticker／markPrice／OI 合併。下方歷史紀錄詳列本地對深度及成交的檢查。 |
| `02_normalize_tardis_bybit.py` | 使用當期 V5 分支。publicTrade 取 i/p/v/S/T。orderbook.50 取外層 ts 建簿。orderbook.1 的缺側在 delta 沿用、在 snapshot 清空。永續 tickers 合併非空值並保留最高已見事件時間。allLiquidation 取 p/v/T，Buy 持倉對應 sell 清算。 |
| `02_normalize_tardis_okx.py` | 目前 V5 的 books 更新以 ts 分檔，一檔原生行情來自 bbo-tbt。trades 取 tradeId/px/sz/side/ts，保留永續合約張數。永續以 tickers.last、markPx、oi、fundingRate、fundingTime、可選 nextFundingRate 更新狀態。 |
| `02_normalize_tardis_coinbase.py` | matches 僅取 match 並反轉 maker side，ticker 提供原生 BBO，缺側留白。ISO 時間保留至微秒。snapshot 與 ticker 時間缺失或早於 epoch 時用接收時間，l2update 時間無效時沿用同來源上次有效增量時間。 |
| `02_normalize_tardis_upbit.py` | 成交取 sequential_id、trade_price、trade_volume、ask_bid、trade_timestamp。每份 orderbook 依正價位重建完整快照，時間取 timestamp。與 handy.ts 相同，數值大於 10 的 15 次方視為微秒，其餘視為毫秒。 |

**本地差異與來源限制**

- 各入口以 supervisor 提交順序或舊格式的 seq 重播。local_timestamp 保留原始接收微秒。價格與數量使用 Decimal。時間窗只限制寫出的列。跨 run 丟棄簿與 ticker 狀態，並保留成交索引。
- Bybit、OKX、Coinbase 與 Upbit 的成交以市場、商品、原生 ID 及映射後的時間、價、量、方向去除相同列。同 ID 的映射欄位有差異時保留並計數。Binance 延用原規則按成交 ID 去重。
- quotes 對應深度一檔、間隔零的簿快照。絕對數量零刪檔，最佳價或量變更才輸出。原生 book_ticker 有獨立來源。OKX 檢查 prevSeqId 接合，維護重置扣回前序仍可套用，有缺口則等新快照。Bybit 不要求 u 逐一遞增。Bybit 和 Upbit 防止較晚抵達的舊深度訊息回滾已發布的報價。
- Coinbase 現有 level2_batch 為合批資料，沒有可靠連續序號。兩路各自從 snapshot 建簿，每商品保留最近活躍的最多三個來源。跨路按原生時間推進，同時間採同來源後續更新。合批邊界與來源選擇會影響 quotes 的列數。
- OKX 現有 trades 已聚合，上游當期預設選取 trades-all。採集未包含 index-tickers 或清算頻道，因此 swap 的 index_price 留空，沒有清算 CSV。books 是公開深度來源。特權來源的更新頻率可能不同。
- 裸 JSONL 的來源健康、靜默、衝突與缺口仍須核對。上游 commit 與實際使用的本地程式 SHA-256 均保存在每家的 normalization_report.json。測試見 [各交易所固定案例](../tests/test_normalize_markets.py) 及 [Binance 回歸](../tests/test_pipeline.py)。

### 重播驗證

以五家既有原生 channel 匯出的 `raw_message`、封裝欄位及 `capture_seq` 還原短測輸入，核對每家序號連續且總數等於原匯出報告。共重播 134,991 筆，建立 92 個 CSV、124,304 列。重播兩次逐檔位元組相同。Binance 的 32 個檔案另與拆分前的入口逐檔比對相同。28 組市場與資料類型的欄位名稱及順序與本地 Tardis 樣本相符。逐列內容仍須使用相同交易日與重疊時間窗對帳。

## 先前的 Binance 核對紀錄

核對日期：2026-11-24（Asia/Shanghai）。本次從 GitHub API 解析 master，並重新下載同一 commit 的四個原始檔。

固定 commit：`8b27eac3bc32a1007430b340983039d623c04dc4`。授權見 [LICENSE](LICENSE)。

每次修改正規化皆須重新讀取 GitHub，規則見 [../AGENTS.md](../../AGENTS.md)。離線重播不需要網路。

| 本地參考檔 | 固定 GitHub 原碼 | SHA-256 |
|---|---|---|
| [binance.ts](binance.ts) | [上游原碼](https://github.com/tardis-dev/tardis-node/blob/8b27eac3bc32a1007430b340983039d623c04dc4/src/mappers/binance.ts) | `819260e0bee154a61b8dcbfb448bfa65276c14db4b57999d69528ba7daa6bf9e` |
| [mapper.ts](mapper.ts) | [上游原碼](https://github.com/tardis-dev/tardis-node/blob/8b27eac3bc32a1007430b340983039d623c04dc4/src/mappers/mapper.ts) | `15221302ca9a469b2c6aa8c3d51662dafb44c7644f452da340046eefb0559f62` |
| [booksnapshot.ts](booksnapshot.ts) | [上游原碼](https://github.com/tardis-dev/tardis-node/blob/8b27eac3bc32a1007430b340983039d623c04dc4/src/computable/booksnapshot.ts) | `ff176e69039a3c9d85de13973013a677e8e189fe892d4a07df341ed9d07770a1` |
| [orderbook.ts](orderbook.ts) | [上游原碼](https://github.com/tardis-dev/tardis-node/blob/8b27eac3bc32a1007430b340983039d623c04dc4/src/orderbook.ts) | `960febd312668d6a43ec38d95d2c15abb9bd374338dadf8dbaae8701bd29b576` |

原始指定連結：[Binance mapper](https://github.com/tardis-dev/tardis-node/blob/master/src/mappers/binance.ts)、[ticker helper](https://github.com/tardis-dev/tardis-node/blob/master/src/mappers/mapper.ts)。

## 本次逐項比對

與先前本地參考版本比較：binance.ts 的差異是 getFilters 的空陣列預設值及 index symbol 轉換；mapper.ts 新增篩選器介面說明。成交、清算、bookTicker、深度緩存與 ticker 狀態方法的映射行為未變。

| 項目 | 上游對應位置 | 本地核對結果 |
|---|---|---|
| trades | BinanceTradesMapper.map | t/p/q/T、m 方向與 INSURANCE_FUND/ADL/NA 過濾相同；本地另外以市場／商品／ID 去重。 |
| book_ticker | BinanceBookTickerMapper.map | a/A/b/B 映射相同；E 缺失時沿用接收時間。 |
| liquidations | BinanceLiquidationsMapper.map | 僅 FILLED；使用 p/z/S/T，id 留空。 |
| derivative_ticker | BinanceFuturesDerivativeTickerMapper.map、PendingDerivativeTickerInfo | c、p/i/r/T、openInterest 合併；值改變才輸出；OI 不改事件時間；時間不倒退；未知欄位保持空值。 |
| 初始 depth | BinanceBookChangeMapper.map、BinanceFuturesBookChangeMapper.mapBookDepthUpdate | 緩存先於快照的 depth，按序號銜接並以絕對數量套用，零數量刪檔；期貨亦接受 pu 直接銜接快照序號。 |
| 快照時間 | BinanceBookChangeMapper.map | 緩存套用後的快照列仍採 snapshot.T，沒有 T 則採快照接收時間；不是改用最後緩存 depth.E。 |
| quotes | BookSnapshotComputable：depth=1、interval=0；OrderBook.update | 以最佳買賣價／量變更觸發一檔輸出。此處核對的是公開計算邏輯，未取得 Tardis 私有 CSV 生產管線，不能據此宣稱每列完全相同。 |

## 有意保留的差異與邊界

- 本地更新序號會持續推進並檢查每一筆 U/u 或 pu/u；遇缺口停止 quotes。上游這兩個 mapper 的 lastUpdateId 主要作快照邊界與首筆檢查，沒有與本地相同的逐筆缺口閘門。
- 上游在同一 mapper 狀態處理快照後忽略額外快照；本地允許同 session 的定期 generation 換代，候選快照接合成功才取代有效舊簿。
- 上游可先產生 snapshot book_change，歷史重播亦可容忍首筆無重疊；本地要求快照接合完成，並拒絕未知覆蓋、空邊或交叉盤，不為取得更多列而放寬檢查。
- 上游初始 CircularBuffer 容量為 2000；本地離線容量為 20000，超限會記錄錯誤並停止該候選簿，避免靜默丟棄。採集端不緩存或建立訂單簿。
- 上游使用 JavaScript Number；本地保留 Decimal，且 JSON 浮點字面值直接解析為 Decimal，避免先經二進位浮點失真。
- 本地採用 capture.time_unit（spot WS 微秒、futures WS／REST 毫秒）明確換算 UTC epoch 微秒；funding_timestamp 的 T 為毫秒。上游使用自己的日期 helper；接收時間來自各自採集端，不能逐列相等。
- 本地以原始連線／REST 錯誤事件重置相應狀態，並在跨 run 時重置；這些事件來自本地採集封裝，不是上游兩份 mapper 的輸入格式。
- 目前只涵蓋 Binance spot 及 USD-M 的既定來源；不宣稱涵蓋上游的 COIN-M、額外 indexPrice 來源或所有歷史市場分支。

## 本次程式調整

- Side、Book、BookEngine 及 BookError 由 common.py 移入 02_normalize_tardis.py；01_collect 與 Binance collector 不再建立或呼叫訂單簿。
- 採集端只保存原文、觀察更新序號、排程初始／定期／缺口快照；status 不再把採集健康描述為 book_synced。
- 正規化報告保存上游 commit／原碼雜湊及本地實作雜湊；本次保留已有嚴格 quotes 輸出規則。
- 回歸驗證包含原始重複／缺口／交叉價位完整保存、定期快照、離線重建／停止／恢復、快照時間、ticker 時間與空值、精確數值，以及完整八商品市場管線。

## Supervisor 合併封裝第三版（2026-11-24 再核對）

本次重新取得上述四個原始檔並閱讀快照、狀態與時間實作；四個 SHA-256 與上一個固定版本相同。第三版 JSONL 只保留整合事件，以邏輯 session 取代副本 session，原接收 session 留在 source_session。第 02 階段依 request_started_us 從有界深度歷史建立快照候選，不依賴 worker 的 snapshot_begin 或斷線控制事件。候選緩存按原生 u 排序，正常事件仍按提交 seq 重播；較晚到達的缺口事件不會回溯改寫已輸出 quotes，缺口後由新快照重新建立可信狀態。這是本地可靠採集封裝的適配，不是上游私有 CSV 管線。第 01 階段依然不建立 L2。

## 可靠性修復再核對（2026-11-24）

本次再次透過 GitHub API 解析 master，下載並閱讀同一 commit `8b27eac3bc32a1007430b340983039d623c04dc4` 的四份原碼；SHA-256 全部與上表相同。修復僅新增差異分類 metadata、第三版 gzip 讀取及持久化／交付機制，不改變原文、原生事件身分、價格／數量、E／T 單位、local_timestamp、空值、提交順序或第 02 階段快照／ticker 狀態規則。所有不同原文仍保留；分類不作行情語義去重。

驗證包含既有五類 Binance 映射、完整八商品市場管線、封存壓縮前後序號與 CSV 位元組一致、CSV 游標提交前崩潰恢復、歷史索引遲到重送與序號鏈查詢。前述與上游的嚴格缺口、候選快照及 Decimal 差異繼續保留，不宣稱與 Tardis 接收時間或逐列內容相同。
