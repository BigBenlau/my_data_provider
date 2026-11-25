# JSON 來源涵蓋清單

本次只沿用既有原生 channel。下列差集包含歷史別名、額外統計及尚未實作來源；不自動替換或新增。

| 市場 | JSON 中尚未實作的來源 |
|---|---|
| binance | `aggTrade`、`ticker`、`borrowInterest`、`recentTrades`、`tokenNav` |
| binance-futures | `premiumIndex`、`indexConstituents`、`fundingInfo`、`insuranceBalance`、`aggTrade`、`compositeIndex`、`assetIndex`、`topLongShortAccountRatio`、`topLongShortPositionRatio`、`globalLongShortAccountRatio`、`takerlongshortRatio`、`!contractInfo`、`recentTrades` |
| bybit | `orderbook.rpi`、`orderbook.full`、`orderbook.500`、`orderbook.1000`、`trade`、`orderBookL2_25`、`orderBook_200`、`liquidation`、`instrument_info`、`insurance`、`long_short_ratio` |
| bybit-spot | `orderbook.200`、`lt`、`trade`、`depth`、`bookTicker` |
| okex | `trades-all`、`books-l2-tbt`、`interest-rate-loan-quota`、`vip-interest-rate-loan-quota`、`status`、`instruments`、`taker-volume`、`liquidations`、`public-block-trades`、`public-struc-block-trades`、`loan-ratio`、`spot/trade`、`spot/depth`、`spot/depth_l2_tbt`、`spot/ticker`、`system/status`、`margin/interest_rate` |
| okex-swap | `trades-all`、`books-l2-tbt`、`liquidations`、`liquidation-orders`、`index-tickers`、`price-limit`、`status`、`instruments`、`long-short-account-ratio`、`taker-volume`、`public-block-trades`、`public-struc-block-trades`、`taker-volume-contract`、`long-short-account-ratio-contract-top-trader`、`long-short-position-ratio-contract-top-trader`、`long-short-account-ratio-contract`、`swap/trade`、`swap/depth`、`swap/depth_l2_tbt`、`swap/ticker`、`swap/liquidation`、`swap/funding_rate`、`swap/mark_price`、`index/ticker`、`system/status`、`information/sentiment`、`information/long_short_ratio`、`information/margin` |
| coinbase | `received`、`open`、`done`、`change`、`full_snapshot`、`rfq_matches` |
| upbit | `ticker` |

Binance 的 JSON `depth` 對應現貨 `depth@100ms`／期貨 `depth@0ms`；`markPrice` 對應 `markPrice@1s`。Coinbase JSON 使用事件名稱，訂閱名稱仍為 `matches`、`level2_batch`、`ticker`。

Binance liquidation 的 JSON「latest」與 step1「largest」差異仍保留：採集原生 forceOrder，不嘗試從公開推送重建被交易所篩掉的事件。
