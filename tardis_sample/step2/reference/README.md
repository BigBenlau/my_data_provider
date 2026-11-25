# Tardis 原碼核對紀錄

核對日期：2026-11-24（Asia/Shanghai）。本次從 GitHub API 解析 master，並重新下載同一 commit 的四個原始檔。

固定 commit：`8b27eac3bc32a1007430b340983039d623c04dc4`。授權見 [LICENSE](LICENSE)。

每次修改正規化皆須重新讀取 GitHub，規則見 [../AGENTS.md](../AGENTS.md)。離線重播不需要網路。

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
