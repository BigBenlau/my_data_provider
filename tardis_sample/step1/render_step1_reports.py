"""Render Chinese/English delivery reports from the same empirical result objects."""
import json
from pathlib import Path
from step1_empirical import utc

ROOT=Path(__file__).resolve().parent
ORDER=['binance-futures','bybit','okex-swap','binance','bybit-spot','okex','coinbase','upbit']
ASSETS=['BTC','ETH','SOL','GRT']
KINDS=['book_ticker','quotes','trades','liquidations','derivative_ticker']
FIELDS=['funding_timestamp','funding_rate','predicted_funding_rate','open_interest','last_price','index_price','mark_price']
CHANGELOG='https://developers.binance.com/zh-CN/docs/products/derivatives-trading-coin-futures/change-log'

def asset(symbol):return next(a for a in ASSETS if a in symbol)
def num(v):return '—' if v is None else f'{v:,}'
def sec(v):return '—' if v is None else f'{v/1e6:.6f}'
def ms(v):return '—' if v is None else f'{v/1000:.6f}'
def pct(n,d):return '—' if not d else f'{100*n/d:.4f}%'
def clean(v):return str(v).replace('|','\\|').replace('\n','<br>')


def render(data,english=False):
    def t(zh,en):return en if english else zh
    out=[]
    def add(s):out.append(s+'\n\n')
    def table(headers,rows):
        out.append('| '+' | '.join(headers)+' |\n| '+' | '.join(['---']*len(headers))+' |\n')
        for row in rows:out.append('| '+' | '.join(clean(v) for v in row)+' |\n')
        out.append('\n')
    files={(f['exchange'],asset(f['symbol']),f['kind']):f for f in data['files']}
    def f(ex,a,k):return files.get((ex,a,k))
    add(t('# Step 1：歷史市場資料分析報告','# Step 1: Historical Market Data Analysis'))
    add(t('本報告先以 2026-09-18 單日完整資料完成 Step 1 方法與初步分析，後續補充另外兩個完整日進行穩健性驗證。研究幣種為 BTC、ETH、SOL、GRT。','This report uses the complete daily files for 2026-09-18 to establish the Step 1 methods and initial findings. Two additional complete days will provide subsequent robustness validation. The assets are BTC, ETH, SOL and GRT.'))
    add(t('## Section 1：資料下載與研究範圍','## Section 1: Data acquisition and study coverage'))
    add(t('已建立 [download_csv.py](../download_csv.py)，可批次設定多個交易所、symbols、data types 與日期區間，下載要求內容。本日取得八市場共 116 份壓縮 CSV；GRT 為選定的相對小眾 token。','[download_csv.py](../download_csv.py) supports batch downloads with per-exchange symbols, data types and date ranges. The requested coverage was downloaded as 116 compressed CSV files across eight markets for the study day. GRT is the selected smaller token.'))
    rows=[]
    for ex in ORDER:
        syms=[next(r['symbol'] for r in data['files'] if r['exchange']==ex and asset(r['symbol'])==a) for a in ASSETS]
        rows.append([ex,' / '.join(syms),', '.join(k for k in KINDS if f(ex,'BTC',k))])
    table([t('市場','Market'),'Symbols',t('CSV 類型','CSV types')],rows)
    add(t('下載保留 `datasets/<exchange>/<data_type>/<date>_<symbol>.csv.gz` 架構。價格報價包含 USDT、USD、KRW；合約數量單位亦可能不同，原始 amount 不直接跨市場加總。','Downloads retain the `datasets/<exchange>/<data_type>/<date>_<symbol>.csv.gz` structure. Quote currencies include USDT, USD and KRW. Contract quantity units also differ; raw amounts are not directly aggregated across markets.'))
    add(t('## Section 2：CSV 欄位、空值與每日資料量','## Section 2: CSV fields, missing values and daily volumes'))
    add(t('### 2.1 欄位定義','### 2.1 Field definitions'))
    add(t('以下依 [Tardis Data Types](https://docs.tardis.dev/downloadable-csv-files/data-types) 整理。完整 schema 為共通欄位加上類型欄位；空白不等於零。','Definitions follow [Tardis Data Types](https://docs.tardis.dev/downloadable-csv-files/data-types). Each schema consists of common fields plus type-specific fields. Missing values are not zero.'))
    add(t('**五類 CSV 的共通欄位**','**Common fields across the five CSV types**'))
    table(['Column',t('含義','Meaning')],[['exchange',t('Tardis 市場 ID','Tardis market ID')],['symbol',t('商品代碼','Instrument ID')],['timestamp',t('交易所事件時間；未提供時可回退至接收時間，UTC epoch 微秒','Exchange event time, with receive-time fallback when unavailable; UTC epoch microseconds')],['local_timestamp',t('Tardis 接收時間，UTC epoch 微秒','Tardis receive time; UTC epoch microseconds')]])
    add(t('**book_ticker 與 quotes：**前者來自原生 WebSocket BBO feed，後者從 L2 訂單簿重建；欄位相同，但暴露的狀態和到達時機可以不同。','**book_ticker and quotes:** the former comes from native WebSocket BBO feeds; the latter is reconstructed from L2 books. Their columns are identical, but exposed states and arrival times can differ.'))
    table(['Column',t('含義','Meaning')],[[k,t(z,e)] for k,z,e in [('ask_amount','最優賣量','Best ask quantity'),('ask_price','最優賣價','Best ask price'),('bid_price','最優買價','Best bid price'),('bid_amount','最優買量','Best bid quantity')]])
    add('**trades / liquidations**')
    table(['Column','trades','liquidations'],[['id',t('成交識別碼','Trade ID'),t('強平識別碼','Liquidation ID')],['side',t('主動成交方 buy / sell / unknown','Aggressor side: buy / sell / unknown'),t('buy：空倉強平；sell：多倉強平；或 unknown','buy: short liquidation; sell: long liquidation; or unknown')],['price',t('成交價格','Execution price'),t('強平價格','Liquidation price')],['amount',t('原生成交數量','Native trade quantity'),t('原生強平數量','Native liquidation quantity')]])
    add('**derivative_ticker**')
    definitions=[('funding_timestamp','下一次 funding event 時間，UTC epoch 微秒','Time of the immediately upcoming funding event; UTC epoch microseconds'),('funding_rate','當前對下一次 funding event 的費率；結算前仍可變動，不能提前視為 final rate','Current rate for the next funding event; it can change before settlement and must not be treated as final in advance'),('predicted_funding_rate','下一次之後再下一次 funding event 的預測費率；未提供時為空','Estimated rate for the funding event following the next one; empty when unavailable'),('open_interest','未平倉量','Open interest'),('last_price','該衍生品合約最近一筆實際成交的價格；初始化或多來源狀態合併時可能為空，不代表沒有成交','Price of the most recent actual trade in the derivative contract; it may be missing during initialization or multi-source state merging, which does not imply no trading occurred'),('index_price','交易所提供的標的資產現貨參考指數價格，按其現貨市場取價規則計算；不是合約自身成交價，也不是 Tardis 自行計算','Exchange-provided spot reference index price for the underlying asset, calculated using the exchange’s spot-market methodology; neither the contract’s traded price nor an index calculated by Tardis'),('mark_price','交易所依指數價格、溢價／基差等機制計算的風控價格，通常用於未實現盈虧與強平判定；不等於最新成交價','Exchange-calculated risk-management price based on the index price and mechanisms such as premium/basis, typically used for unrealized P&L and liquidation assessment; distinct from the last traded price')]
    table(['Column',t('含義','Meaning')],[[k,t(z,e)] for k,z,e in definitions])
    add(t('### 2.2 空值觀察','### 2.2 Missing-value observations'))
    add(t('本日 trades、quotes、book_ticker 欄位沒有空值；可讀強平檔的 id 全空。三個永續市場的 predicted_funding_rate 均全空，不能推廣為全部歷史。Bybit 其餘衍生品欄位均有值；Binance 和 OKX 有少量空值。','There are no missing fields in trades, quotes or book_ticker on the study day. Liquidation IDs are entirely missing in readable records. predicted_funding_rate is entirely missing at all three perpetual venues; this does not establish its historical availability. Other derivative fields are fully populated at Bybit, with small missing counts at Binance and OKX.'))
    table([t('市場','Market'),'Field',t('空值筆數 BTC / ETH / SOL / GRT','Missing rows: BTC / ETH / SOL / GRT')],[[ex,k,' / '.join(str(f(ex,a,'derivative_ticker')['fields'][k]['empty']) for a in ASSETS)] for ex in ORDER[:3] for k in FIELDS if k!='predicted_funding_rate'])
    add(t('OKX GRT-USDT-SWAP 的 liquidation 壓縮檔為 20 bytes，gzip 完整讀取驗證成功，解壓為 0 bytes。這是空文件，不是解析錯誤；Tardis 此日檔沒有記錄到該商品的強平事件，不表示沒有真實強平。','The OKX GRT-USDT-SWAP liquidation archive is 20 bytes, passes complete gzip reading, and decompresses to 0 bytes. It is an empty file, not a parsing error. The Tardis daily file contains no recorded liquidation events for this instrument; this does not establish the absence of real liquidations.'))
    add(t('### 2.3 每日資料列數','### 2.3 Daily row counts'))
    rows=[]
    for ex in ORDER:
        for a in ASSETS:
            vals=[]
            for k in KINDS:
                r=f(ex,a,k);vals.append('—' if r is None else (t('空文件（0）','Empty file (0)') if r['status']=='empty_file' else num(r['rows'])))
            rows.append([ex,a]+vals)
    table([t('市場','Market'),t('幣種','Asset')]+KINDS,rows)
    add(t('### 2.4 市場差異','### 2.4 Differences between venues'))
    add(t('Binance 的 book_ticker 比 quotes 密集，Coinbase 則相反。Coinbase BTC 有 2,840,880 筆 quotes、605,303 筆 book_ticker；四幣種 book_ticker 均比 trades 多一列。GRT 通常比其他三幣種稀疏。這些是來源與活躍度差異，不直接表示資料品質差異。','Binance book_ticker is denser than quotes, whereas Coinbase exhibits the opposite pattern. Coinbase BTC has 2,840,880 quote rows and 605,303 book_ticker rows; book_ticker has one more row than trades for each of the four assets. GRT is generally sparser than the other assets. These are source and activity differences, not direct quality rankings.'))
    add(t('## Section 3：核心實證分析','## Section 3: Empirical findings'))
    add(t('### 3.1 Binance perpetual：BBO 覆蓋、到達先後及分歧','### 3.1 Binance perpetuals: BBO coverage, arrival order and disagreement'))
    add(t('使用完整日四欄狀態 `(bid_price, bid_amount, ask_price, ask_amount)`，精確十進位比較；100 與 100.0 相等。原始列數另列，連續相同狀態壓縮為首次 local_timestamp。','States use all four fields `(bid_price, bid_amount, ask_price, ask_amount)` with exact decimal equality; 100 and 100.0 are equal. Raw row counts are retained separately. Consecutive identical states are collapsed to their first local_timestamp.'))
    table([t('幣種','Asset'),'quotes','book_ticker',t('筆數比','Row ratio'),t('BBO 狀態出現次數','BBO occurrences'),t('quote 狀態出現次數','Quote occurrences')],[[b['asset'],num(b['rows']['quotes']),num(b['rows']['book_ticker']),f"{b['rows']['book_ticker']/b['rows']['quotes']:.2f}",num(b['occurrences'].get('0',b['occurrences'].get(0))),num(b['occurrences'].get('1',b['occurrences'].get(1)))] for b in data['bbo']])
    add(t('**Coverage：**以下分母為各 feed 全日不同狀態數；only 表示另一 feed 全日未出現該精確狀態，不代表可證明每筆都是因 L2 抽樣而遺失的中間狀態。','**Coverage:** denominators are each feed’s distinct daily states. “Only” means the other feed never exposes that exact state during the day. It does not prove every such state was an intermediate state lost through L2 sampling.'))
    table([t('幣種','Asset'),t('BBO 不同狀態','Distinct BBO states'),t('Quote 不同狀態','Distinct quote states'),t('共有狀態','Shared states'),'Shared/BBO %','Shared/Quote %','BBO-only','BBO-only %','Quote-only','Quote-only %'],[[b['asset'],num(b['distinct']['book']),num(b['distinct']['quotes']),num(b['distinct']['shared']),pct(b['distinct']['shared'],b['distinct']['book']),pct(b['distinct']['shared'],b['distinct']['quotes']),num(b['distinct']['book_only']),pct(b['distinct']['book_only'],b['distinct']['book']),num(b['distinct']['quote_only']),pct(b['distinct']['quote_only'],b['distinct']['quotes'])] for b in data['bbo']])
    add(t('**Arrival：**同狀態按時間順序一對一 FIFO 配對，主窗口 ±1 秒；另測 ±100ms、±5 秒。先到者超出窗口才移除，不預設領先方向。窗口內雙方皆只有一個候選的配對列為 unique；候選數在原始序列計算，不因先前已配對而刪減。Δ = quote.local_timestamp − book_ticker.local_timestamp，正值代表 book_ticker 較早。','**Arrival:** identical states are paired one-to-one in chronological FIFO order within ±1 second, with ±100ms and ±5-second sensitivity checks. An earlier event is discarded only when outside the window; no lead direction is assumed. A pair is unique only when each event has one candidate in the original opposite sequence. Δ = quote.local_timestamp − book_ticker.local_timestamp; positive Δ means book_ticker arrives earlier.'))
    rows=[];metrics=[]
    for b in data['bbo']:
        nb=b['occurrences'].get('0',b['occurrences'].get(0));nq=b['occurrences'].get('1',b['occurrences'].get(1))
        for w in ['100000','1000000','5000000']:
            st=b['windows'][w];n=st['all']['n'];u=st['unique']['n']
            rows.append([b['asset'],int(w)/1000,num(n),pct(n,nb),pct(nb-n,nb),pct(n,nq),pct(nq-n,nq),pct(n-u,n)])
            for category in ['all','unique']:
                r=st[category];metrics.append([b['asset'],int(w)/1000,category,num(r['n']),pct(r['book_first'],r['n']),pct(r['quotes_first'],r['n']),pct(r['same'],r['n']),ms(r['median_us']),ms(r['p95_us']),ms(r['abs_p95_us'])])
    table([t('幣種','Asset'),t('窗口 ±ms','Window ±ms'),t('配對數','Pairs'),'BBO matched %','BBO unmatched %','Quote matched %','Quote unmatched %',t('歧義/配對 %','Ambiguous/pairs %')],rows)
    table([t('幣種','Asset'),t('窗口 ±ms','Window ±ms'),t('子集','Subset'),'N','BBO first %','Quotes first %','Same %','Median Δ ms','p95 Δ ms','p95 abs(Δ) ms'],metrics)
    for b in data['bbo']:
        r=b['windows']['1000000']['unique'];lead='book_ticker' if r['book_first']>r['quotes_first'] else 'quotes'
        add(t(f"{b['asset']}：±1 秒唯一候選配對中，book_ticker 先到 {pct(r['book_first'],r['n'])}，quotes 先到 {pct(r['quotes_first'],r['n'])}；較常先到的是 {lead}。此結論僅適用於可配對子集，不推論每筆事件。",f"{b['asset']}: in unique-candidate ±1-second pairs, book_ticker arrives first in {pct(r['book_first'],r['n'])} and quotes in {pct(r['quotes_first'],r['n'])}; {lead} more often arrives first. This conclusion concerns the matched subset, not every event."))
    add(t('**時間加權分歧：**同接收時間的更新先全部處理，再積分到下一時刻；分母為雙方都有狀態、且未超出任一 feed 最後觀測時間的共同區間。價格相同但數量不同獨立列出。長時間沿用狀態仍計入，不能把它單獨視為交易所延遲。','**Time-weighted disagreement:** all updates at the same receive time are applied before integrating to the next instant. The denominator is the common period in which both states exist, ending at the earlier last observation. Quantity-only disagreement is reported separately. Carried-forward states remain included and are not independently interpreted as exchange latency.'))
    table([t('幣種','Asset'),t('共同觀測秒','Common seconds'),'Bid price %','Ask price %','Any price %','Same price, different amount %'],[[b['asset'],sec(b['disagreement']['observed_us'])]+[pct(b['disagreement'][k],b['disagreement']['observed_us']) for k in ['bid_us','ask_us','any_price_us','amount_only_us']] for b in data['bbo']])
    add(t('**可追溯配對例子（唯一候選，±1 秒）：**記錄編號從 1 開始，不含表頭。','**Traceable examples (unique candidates, ±1 second):** record numbers start at 1 and exclude the header.'))
    table([t('幣種','Asset'),'State (bid price, bid amount, ask price, ask amount)','BBO record','Quote record','BBO local UTC','Quote local UTC','Δ ms'],[[b['asset'],', '.join(e['state']),e['book_record'],e['quote_record'],utc(e['book_time']),utc(e['quote_time']),ms(e['delta_us'])] for b in data['bbo'] for e in b['windows']['1000000']['examples'][:2]])
    add(t('### 3.2 derivative_ticker：每欄實際值變更頻率','### 3.2 derivative_ticker: observed field-value change frequency'))
    add(t('初次有效值不算變更；相同數值不算，空值中斷比較並重置初始化。間隔按 local_timestamp 計算，不跨空值段；少於兩次變更顯示 —。非空比例以全部資料列為分母，所有分位數採 nearest-rank。這是可觀察的值變更頻率，不是上游重發相同值的頻率。','Initial valid values are not changes; numerically equal values are not changes. Nulls reset initialization. Intervals use local_timestamp and do not cross null segments; fewer than two changes yields —. Non-null percentages use all rows; all quantiles use nearest-rank. These measure observed value changes, not upstream transmissions repeating the same value.'))
    table([t('市場','Market'),t('幣種','Asset'),'Field','Non-null %','Changes/day','Median interval s','p95 interval s'],[[ex,a,k,pct(f(ex,a,'derivative_ticker')['fields'][k]['filled'],f(ex,a,'derivative_ticker')['rows']),num(f(ex,a,'derivative_ticker')['changes'][k]['count']),sec(f(ex,a,'derivative_ticker')['changes'][k]['median_us']),sec(f(ex,a,'derivative_ticker')['changes'][k]['p95_us'])] for ex in ORDER[:3] for a in ASSETS for k in FIELDS])
    add(t('predicted_funding_rate 全空；某欄非空但變更為 0 表示觀測值固定，不表示沒有收到訊息。來源頻率見 Section 4。整表平均密度僅作補充：','predicted_funding_rate is entirely missing. A populated field with zero changes is constant in the observations, not necessarily untransmitted. Source frequencies appear in Section 4. Whole-table average densities are supplementary:'))
    table([t('市場','Market'),t('幣種','Asset'),'Rows/day','Rows/s'],[[ex,a,num(f(ex,a,'derivative_ticker')['rows']),f"{f(ex,a,'derivative_ticker')['rows']/86400:.3f}"] for ex in ORDER[:3] for a in ASSETS])
    for ex in ORDER[:3]:
        changes=f(ex,'BTC','derivative_ticker')['changes']
        add(t(f"{ex} BTC：OI 值變更間隔中位數 {sec(changes['open_interest']['median_us'])} 秒，mark_price 為 {sec(changes['mark_price']['median_us'])} 秒，funding_rate 為 {sec(changes['funding_rate']['median_us'])} 秒。不同欄位的更新速度不能用整表列數代替。",f"{ex} BTC: median observed change intervals are {sec(changes['open_interest']['median_us'])} seconds for OI, {sec(changes['mark_price']['median_us'])} seconds for mark_price, and {sec(changes['funding_rate']['median_us'])} seconds for funding_rate. Whole-table row counts cannot substitute for these field-specific rates."))
    add(t('### 3.3 liquidations：發布語義','### 3.3 liquidations: publication semantics'))
    table([t('市場','Market'),'Channel',t('解讀','Interpretation')],[['Binance','forceOrder',t('每 symbol 每 1000ms window 最多一筆；研究日按 largest liquidation order 解讀','At most one selected order per symbol per 1000ms window; use largest liquidation order semantics for the study date')],['Bybit','allLiquidation',t('metadata 標示 500ms 批次推送，不等於每批只有一筆','Metadata specifies 500ms batch pushes, not necessarily one event per batch')],['OKX','liquidations / liquidation-orders',t('metadata 兩來源均指向 liquidation，描述每合約每秒最多一次','Both sources are mapped to liquidation in metadata, with at most one update per contract per second')]])
    add(t(f'Binance [官方 changelog]({CHANGELOG}) 的 2026-04-10 條目明確針對 USDⓈ-M，生效日 2026-04-14，將描述從 latest 改為 largest。因此 2026-09-18 採官方當期定義；Tardis JSON 原文仍為 latest，於 Section 4 原樣保留並註明差異。這是發布語義／描述更新，不能從公開 feed 還原完整逐筆強平。',f'The Binance [official changelog]({CHANGELOG}) entry dated 2026-04-10 explicitly concerns USDⓈ-M and takes effect on 2026-04-14, changing the description from latest to largest. The official definition applicable on 2026-09-18 is used here. Tardis JSON still says latest; Section 4 preserves that original wording and notes the discrepancy. This publication-description update does not make the feed a complete liquidation tape.'))
    add(t('OKX GRT 空檔表示沒有可用的當日強平記錄。不同市場公開篩選、批次方式及數量單位不同，不能將列數直接比較成真實強平總量。','The empty OKX GRT file contains no available liquidation records for the day. Venue-specific selection, batching and quantity units prevent interpreting row counts as directly comparable total real liquidations.'))
    add(t('### 3.4 逐檔 >5 秒 gap 與事故對照','### 3.4 Per-file gaps over 5 seconds and incident overlap'))
    add(t('以每個檔案有效 timestamp 排序去重後的相鄰事件差計算，嚴格 >5 秒；原始 timestamp 倒序另計。事故判定採與本地 incidentReports 的時間交集；八市場當日均無重疊的事故紀錄。沒有重疊不等於沒有故障；事件時間靜默亦不等於採集端接收中斷。','Gaps are strictly greater than 5 seconds between consecutive valid event timestamps after sorting and deduplication within each file. Original timestamp reversals are counted separately. Incident matching uses temporal intersection with local incidentReports; none of the eight venues has a report overlapping the study day. No overlap does not prove fault-free collection; event-time silence is not identical to an interruption in message reception.'))
    rows=[]
    for ex in ORDER:
        for a in ASSETS:
            for k in KINDS:
                r=f(ex,a,k)
                if r is None:continue
                rows.append([ex,a,k,t('全天無記錄','No records all day') if r['rows']==0 else num(r['gaps']),sec(r['max_gap_us']),r['incident_matches'],r['backwards'],sec(r.get('day_start_unobserved_us')),sec(r.get('day_end_unobserved_us'))])
    table([t('市場','Market'),t('幣種','Asset'),'Type','Gaps >5s','Max s','Incident matches','Timestamp reversals','Day-start unobserved s','Day-end unobserved s'],rows)
    add(t('所有間隔均逐段計算；主表不展開每段明細。日首／日尾欄以當日首末事件計算未觀測邊界，不混入內部 gap；跨午夜的相鄰間隔另排除。空文件沒有兩個事件端點，不虛構一個內部 gap；整日 86,400 秒另按無記錄解讀。強平與低活躍商品的長 gap 可能正常。','Every interval is evaluated individually; the main table summarizes rather than enumerates them. Day-start/end values use the first and last in-day events to measure unobserved boundaries, excluded from internal gaps; adjacent intervals crossing midnight are also excluded. An empty file has no two event endpoints, so no internal gap is invented; the entire 86,400-second day is classified as having no records. Long gaps can be normal for liquidation feeds and low-activity instruments.'))
    add(t('### 3.5 跨類型共同靜默','### 3.5 Cross-type common silence'))
    add(t('同 exchange/symbol/date 合併 timestamp：永續五類、現貨三類、Upbit 兩類。任一類型有事件即中斷靜默。有效空強平檔不提供事件，但不阻止其他四類合併。此指標補充逐檔 gap，不替代單一 feed 健康檢查；亦排除日界線。','Event timestamps are merged within exchange/symbol/date across five perpetual types, three spot types, or two Upbit types. Any event ends common silence. A valid empty liquidation file contributes no events and does not block merging the other four types. This supplements per-file gaps rather than replacing individual-feed checks; day boundaries are excluded.'))
    table([t('市場','Market'),'Symbol','Common gaps >5s','Max s','Incident matches'],[[r['exchange'],r['symbol'],num(r['count']),sec(r['max_us']),r['incident_matches']] for r in data['common']])
    add(t('## Section 4：交易所 metadata 與開源轉換規則','## Section 4: Exchange metadata and open-source normalization'))
    add(t('訪問 URL：','Access URL: ')+ '`https://api.tardis.dev/v1/exchanges/{exchange}`. '+t('例如','Example')+': [binance-futures](https://api.tardis.dev/v1/exchanges/binance-futures).')
    add(t('已由 [download_exchange_info.py](../download_exchange_info.py) 取得八市場 [JSON](../exchange_info/)，選定商品保留在 availableSymbols／datasets.symbols，其餘欄位按下載內容驗證。channelDetails 記載來源、頻率、版本、生成標記及歷史說明。','[download_exchange_info.py](../download_exchange_info.py) retrieves the eight market [JSON files](../exchange_info/), retaining selected instruments in availableSymbols/datasets.symbols and validating the remaining fields against the downloaded response. channelDetails records sources, frequencies, versions, generated flags and historical notes.'))
    add(t('### 4.1 Channel 與 endpoint','### 4.1 Channels and endpoints'))
    add(t('按 CSV type 分組，空白延續上方類型。trade／liquidation 對應 trades／liquidations，book_change 為 quotes 的 L2 基礎。Endpoint 與 frequency 均依 JSON，不作現行可連線保證；歷史 channel 不一定適用研究日。frequencySource 中 exchange-docs／internal／inferred 分別為文件、採集設定、推斷。未提供的 generated 不等於 false。','Rows are grouped by CSV type; blank type cells continue the group. trade/liquidation map to trades/liquidations, and book_change underlies L2-derived quotes. Endpoints and frequencies are reproduced from JSON, not guaranteed current live connection settings. Historical channels may not apply on the study date. frequencySource values exchange-docs/internal/inferred denote documentation, collection settings and inference. An absent generated flag does not mean false.'))
    maps=[('book_ticker','book_ticker'),('quotes','book_change'),('trades','trade'),('liquidations','liquidation'),('derivative_ticker','derivative_ticker')]
    missing=t('未提供','Not provided')
    for ex in ORDER:
        d=json.loads((ROOT.parent/'exchange_info'/f'{ex}.json').read_text());c=d['dataCollectionDetails']
        add('#### '+ex)
        add(t('依據：','Source: ')+f'[{ex}.json](../exchange_info/{ex}.json)')
        ep=c.get('wssConnection',{});rows=[['WS',ep.get('url',missing),ep.get('apiVersion',missing),t('JSON 採集地址','JSON collection address')]]
        for ep in c.get('wssConnectionChanges',[]):rows.append([t('歷史 WS','Historical WS'),ep.get('url',missing),ep.get('apiVersion',missing),t('截至 ','Until ')+ep.get('until',missing)])
        if ex in ('binance','binance-futures'):rows.append(['REST depthSnapshot',missing,missing,t('JSON 僅描述 REST，未提供完整 URL','JSON describes REST but does not supply its full URL')])
        if ex=='binance-futures':rows.append(['REST openInterest',missing,missing,t('約每 6 秒；JSON 未提供完整 URL','Approximately every 6 seconds; full URL absent from JSON')])
        table([t('類別','Type'),'Endpoint','apiVersion',t('說明','Notes')],rows)
        rows=[]
        for kind,event in maps:
            for i,ch in enumerate(x for x in d['channelDetails'] if event in x.get('sourceFor',[])):
                since=ch.get('availableSince',missing);until=ch.get('availableTo',missing)
                status=t('未列結束','No end specified') if until==missing else t('期間內','Within period')
                if until!=missing and until<='2026-09-18T00:00:00.000Z':status=t('已結束','Ended')
                if since!=missing and since>'2026-09-18T00:00:00.000Z':status=t('尚未開始','Not started')
                name=f"[{ch['name']}]({ch['exchangeDocsUrl']})" if 'exchangeDocsUrl' in ch else ch['name']
                rows.append([kind if i==0 else '',name,ch.get('frequency',missing),ch.get('frequencySource',missing),ch.get('apiVersion',missing),str(ch['generated']).lower() if 'generated' in ch else missing,since+' → '+until,status])
        table(['CSV type','Channel','frequency','frequencySource','apiVersion','generated','availableSince → availableTo (UTC)',t('研究日狀態','Study-date status')],rows)
        rows=[]
        for ch in d['channelDetails']:
            if not ch.get('sourceFor'):continue
            notes=ch.get('description','')
            if ch.get('additionalInfo'):notes+='\nadditionalInfo: '+ch['additionalInfo']
            if ch.get('changes'):notes+='\nchanges: '+json.dumps(ch['changes'],ensure_ascii=False)
            rows.append([ch['name'],notes])
        table(['Channel',t('JSON 原文說明','Original JSON descriptions')],rows)
        if ex=='binance-futures':add(t('forceOrder 的 latest 為 JSON 原文；本報告對研究日採官方 largest 定義，見 Section 3.3。','The forceOrder wording latest is the original JSON text. This report applies the official largest definition to the study date; see Section 3.3.'))
        if ex=='bybit':add(t('orderbook.full／orderbook.rpi 沒有 sourceFor，不自行指定為 quotes 來源。','orderbook.full/orderbook.rpi lack sourceFor and are not independently assigned as quote sources.'))
    add(t('### 4.2 開源 mapper','### 4.2 Open-source mappers'))
    add(t('Tardis [tardis-node](https://github.com/tardis-dev/tardis-node) 的 [src/mappers](https://github.com/tardis-dev/tardis-node/tree/master/src/mappers) 實作原生訊息到 normalized event 的映射。例如 Binance trade 的 t／p／q 對應 id／price／amount。原生資料經 mapper、必要的狀態重建，再輸出 CSV。mapper 不等於整套 CSV 匯出管線，歷史重現需固定版本。','Tardis [tardis-node](https://github.com/tardis-dev/tardis-node) implements native-to-normalized event mappings in [src/mappers](https://github.com/tardis-dev/tardis-node/tree/master/src/mappers). For example, Binance trade fields t/p/q map to id/price/amount. CSV generation follows native messages, mappers and any required state reconstruction. Mappers are not the entire CSV export pipeline; historical reproduction requires version pinning.'))
    return ''.join(out)


if __name__=='__main__':
    data=json.loads((ROOT/'research_results/empirical/empirical.json').read_text())
    if len(data['files'])!=116 or len(data['bbo'])!=4:raise ValueError('Incomplete empirical run')
    for english,name in [(False,'step1_report.md'),(True,'step1_report_en.md')]:
        (ROOT/name).write_text(render(data,english))
