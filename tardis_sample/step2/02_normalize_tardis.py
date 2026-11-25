#!/usr/bin/env python3
"""第 02 階段：離線重播原始紀錄，依來源語義生成 Binance Tardis 格式 CSV。"""
import argparse
import base64
from collections import Counter,OrderedDict,deque
from decimal import Decimal
import csv
import json
import heapq
import hashlib
from pathlib import Path
import sqlite3
from common import (SCHEMAS,TYPES,BBO,ProtocolError,validate,dec,encode,micros,utc,atomic_json,LOG,configure_logging)
from raw import read_records, read_runs, check_count

TARDIS_COMMIT = '8b27eac3bc32a1007430b340983039d623c04dc4'
TARDIS_SOURCES = {'binance.ts': 'src/mappers/binance.ts', 'mapper.ts': 'src/mappers/mapper.ts',
                  'booksnapshot.ts': 'src/computable/booksnapshot.ts', 'orderbook.ts': 'src/orderbook.ts'}


def reference_metadata():
    # 版本與實際原碼雜湊隨輸出保存；GitHub 核對在修改時進行，離線重播不連網。
    root = Path(__file__).parent
    sources = {name: {'url': f'https://github.com/tardis-dev/tardis-node/blob/{TARDIS_COMMIT}/{path}',
                      'sha256': hashlib.sha256((root / 'reference' / name).read_bytes()).hexdigest()}
               for name, path in TARDIS_SOURCES.items()}
    return {'commit': TARDIS_COMMIT, 'reviewed_on': '2026-11-24', 'sources': sources,
            'comparison_notes': 'reference/README.md', 'policy': 'tardis_mapping_with_strict_local_book_checks',
            'implementation_sha256': {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                                      for name in ('02_normalize_tardis.py', 'common.py', 'raw.py')}}


class BookError(ValueError): 
    """訂單簿無法可靠接合或已超出已知快照範圍。"""
    pass

class Side:
    """用價位字典加堆積維護單邊訂單簿；支援零數量刪檔與快速查詢最佳價。"""
    def __init__(self,bid): 
        # 初始化價位容器；買盤反轉符號，讓同一個最小堆積同時支援買賣盤。
        self.values={}; self.heap=[]; self.sign=-1 if bid else 1
    def apply(self,levels):
        # 套用價位絕對數量，零代表刪除；定期清理堆積中的過期價位。
        for p,q in levels:
            p,q=dec(p),dec(q)
            if q==0: self.values.pop(p,None)
            else:
                if p not in self.values: heapq.heappush(self.heap,self.sign*p)
                self.values[p]=q
        if len(self.heap)>2*len(self.values)+1000:
            self.heap=[self.sign*p for p in self.values]; heapq.heapify(self.heap)
    def best(self):
        # 清除過期堆積項目後回傳最佳價／量；空邊視為不可用訂單簿。
        while self.heap and self.sign*self.heap[0] not in self.values: heapq.heappop(self.heap)
        if not self.heap: raise BookError('empty_side')
        p=self.sign*self.heap[0]; return p,self.values[p]


class Book:
    """單一 snapshot 世代的 Binance L2 狀態，包含快照前增量緩存及序號接合。"""
    def __init__(self,market,limit=20000):
        # 初始化兩邊訂單簿、緩存上限、最後更新序號與同步狀態。
        self.market=market; self.limit=limit; self.buffer=[]; self.bids=Side(True); self.asks=Side(False)
        self.last=None; self.synced=False; self.bounds=None
    def snapshot(self,d,local,unit):
        # 套用 REST 快照後重播已緩存 delta；記住快照邊界，避免把未知深度當成完整市場。
        self.last=int(d['lastUpdateId']); self.bids.apply(d['bids']); self.asks.apply(d['asks'])
        if not self.bids.values or not self.asks.values: raise BookError('empty_snapshot')
        self.bounds=(min(self.bids.values),max(self.asks.values))
        buffered,self.buffer=self.buffer,[]
        for update,received,u in buffered: self.update(update,received,u)
        if self.synced: return self.quote(micros(d['T'],unit) if 'T' in d else local,local)
    def update(self,d,local,unit):
        # 按 spot 的 U/u 或 futures 的 pu/u 驗證連續性，再套用 delta。
        if self.last is None:
            self.buffer.append((d,local,unit))
            if len(self.buffer)>self.limit: raise BookError('buffer_overflow')
            return None
        u,U=int(d['u']),int(d['U'])
        if self.market=='binance':
            # 現貨丟棄已套用的增量，新批次必須涵蓋下一個更新序號。
            if u<=self.last: return None
            if U>self.last+1: raise BookError('sequence_gap')
        else:
            if u<self.last or (self.synced and u==self.last): return None
            if not self.synced:
                # 期貨首筆先接合快照；之後以 pu 銜接上一筆 u。
                if not (U<=self.last<=u or int(d['pu'])==self.last): raise BookError('snapshot_overlap')
            elif int(d['pu'])!=self.last: raise BookError('sequence_gap')
        self.bids.apply(d['b']); self.asks.apply(d['a']); self.last=u; self.synced=True
        return self.quote(micros(d['E'],unit),local)
    def quote(self,t,local):
        # 僅從已知有效買賣盤產生 BBO；交叉盤或價格越界時報錯。
        bid,bq=self.bids.best(); ask,aq=self.asks.best()
        if bid>=ask: raise BookError('crossed_book')
        if bid<self.bounds[0] or ask>self.bounds[1]: raise BookError('snapshot_coverage')
        return dict(zip(BBO,(aq,ask,bid,bq)),timestamp=t,local_timestamp=local)


class BookEngine:
    """離線重播同一個 WS session 的目前與候選快照，完成接合才換代。"""
    def __init__(self,market,limit=20000):
        # 建立目前／候選 book 容器與最近一次序號問題紀錄。
        self.market=market; self.limit=limit; self.current=None; self.pending=None; self.generation=None
        self.issue=None
    def begin(self,generation):
        # 啟動指定 generation 的候選快照，保留仍有效的目前 book。
        self.pending=Book(self.market,self.limit); self.generation=generation
    def promote(self,row):
        # 候選 book 同步成功後才取代目前 book，避免快照更新造成假空窗。
        if self.pending and self.pending.synced:
            self.current,self.pending=self.pending,None
            return row
    def snapshot(self,d,local,unit,generation):
        # 只接受相符 generation 的快照；候選失敗不破壞目前有效 book。
        if self.pending is None or generation!=self.generation: return None
        try: return self.promote(self.pending.snapshot(d,local,unit))
        except BookError:
            self.pending=None; raise
    def update(self,d,local,unit):
        # 同時更新目前與候選 book；記錄無效原因，必要時停止可信 BBO 輸出。
        self.issue=None; row=None; promoted=None
        if self.current:
            try: row=self.current.update(d,local,unit)
            except BookError as e: self.issue=str(e); self.current=None
        if self.pending:
            try: promoted=self.promote(self.pending.update(d,local,unit))
            except BookError as e: self.issue=str(e); self.pending=None
        return promoted or row

class Mapper:
    """依固定 Tardis 規則維護每個市場／商品的 book、ticker 與交易去重狀態。"""
    def __init__(self,db):
        # 建立磁碟 SQLite trade ID 索引與可重置的行情狀態，控制長期重播記憶體用量。
        self.db=db;db.execute('CREATE TABLE IF NOT EXISTS seen (exchange TEXT,symbol TEXT,id TEXT,PRIMARY KEY(exchange,symbol,id)) WITHOUT ROWID')
        self.books={};self.tickers={};self.sessions={};self.previous_bbo={};self.issues=Counter()
        self.depth_history={}
    def reset(self):
        # 跨 run 重置 book／ticker/session，但保留跨 run 的交易 ID 去重索引。
        self.books.clear();self.tickers.clear();self.sessions.clear();self.previous_bbo.clear()
        self.depth_history.clear()
    def depth_engine(self, record):
        # 每次 session 變更都捨棄舊訂單簿；記憶體只保留每個商品的目前狀態。
        if not record['channel'].startswith('depth'):
            return None
        pair = record['exchange'], record['symbol']
        if self.sessions.get(pair) != record['session']:
            self.books[pair] = BookEngine(pair[0])
            self.sessions[pair] = record['session']
            self.previous_bbo.pop(pair, None)
            self.depth_history.pop(pair, None)
            if record.get('legacy_schema'):
                self.books[pair].begin(-1)
        return self.books[pair]

    def lifecycle(self, record, engine):
        # 控制事件只改變可信狀態；斷線或快照缺口期間不產生合併後的行情列。
        pair = record['exchange'], record['symbol']
        event, channel = record['event'], record['channel']
        if engine is not None:
            if event == 'snapshot_begin':
                engine.begin(record['generation'])
            elif event in ('session_end', 'disconnected', 'book_gap'):
                self.books[pair] = BookEngine(pair[0])
                self.previous_bbo.pop(pair, None)
                if record.get('legacy_schema'):
                    self.books[pair].begin(-1)
            elif event == 'snapshot_error':
                engine.pending = None
        if channel in ('ticker', 'markPrice@1s', 'openInterest'):
            if event in ('session_end', 'disconnected', 'rest_error', 'rest_failure'):
                self.tickers.pop(pair, None)

    def trade(self, record, data):
        # 僅在第 02 階段過濾指定成交類別，並以市場、商品及成交 ID 去重。
        if data.get('X') in ('INSURANCE_FUND', 'ADL', 'NA'):
            self.issues['off_book_trades'] += 1
            return None
        tid = str(data['t'])
        identity = record['exchange'], record['symbol'], tid
        if not self.db.execute('INSERT OR IGNORE INTO seen VALUES (?,?,?)', identity).rowcount:
            self.issues['duplicate_trades'] += 1
            return None
        return dict(id=tid, side='sell' if data['m'] else 'buy', price=dec(data['p']),
                    amount=dec(data['q']), timestamp=micros(data['T'], record.get('time_unit', 'ms')))

    def quote(self, record, data, engine):
        # 對應上游深度一檔、間隔零的輸出條件；本地另保留嚴格接合檢查。
        local, unit = record['local_timestamp'], record.get('time_unit', 'ms')
        if record.get('merged'):
            self.merged_depth(record, data, engine)
        if record['channel'] == 'depthSnapshot':
            row = engine.snapshot(data, local, unit, record['generation'])
        else:
            row = engine.update(data, local, unit)
            if engine.issue:
                self.issues[engine.issue] += 1
        if row:
            pair = record['exchange'], record['symbol']
            state = tuple(row[k] for k in BBO)
            if state == self.previous_bbo.get(pair):
                return None
            self.previous_bbo[pair] = state
        return row

    def merged_depth(self, record, data, engine):
        # 合併封裝沒有副本生命週期事件；依 REST 請求開始時間重建候選快照緩存。
        pair = record['exchange'], record['symbol']
        history = self.depth_history.setdefault(pair, deque(maxlen=engine.limit))
        if record['channel'] != 'depthSnapshot':
            history.append((data, record['local_timestamp'], record.get('time_unit', 'ms')))
            return
        started = record['request_started_us']
        engine.begin(record['generation'])
        if len(history) == engine.limit and history[0][1] > started:
            engine.pending = None; self.issues['merged_snapshot_history_overflow'] += 1
            return
        # 只在離線階段整理同一快照候選的原生更新序號；不改寫 JSONL 提交順序。
        pending = [item for item in history if item[1] >= started]
        engine.pending.buffer.extend(sorted(pending, key=lambda item: int(item[0]['u'])))

    def ticker(self, record, data):
        # 合併衍生品欄位，未知值保持空白；OI 不可直接替換行情事件時間。
        pair = record['exchange'], record['symbol']
        channel, unit = record['channel'], record.get('time_unit', 'ms')
        state = self.tickers.setdefault(pair, {})
        updates = {}
        if channel == 'ticker':
            updates['last_price'] = dec(data['c'])
        elif channel == 'openInterest':
            updates['open_interest'] = dec(data['openInterest'])
        else:
            updates['mark_price'] = dec(data['p'])
            if 'i' in data:
                updates['index_price'] = dec(data['i'])
            if data.get('r', '') != '' and data.get('T', 0) != 0:
                updates.update(funding_rate=dec(data['r']), funding_timestamp=micros(data['T'], 'ms'))
        if channel != 'openInterest':
            state['timestamp'] = max(state.get('timestamp', 0), micros(data['E'], unit))
        changed = any(state.get(k) != value for k, value in updates.items())
        state.update(updates)
        if changed:
            state.setdefault('timestamp', record['local_timestamp'])
            return dict(state)
        return None

    def map_payload(self, record, data, engine):
        # 按原生 channel 分派固定映射規則；價格與數量始終使用 Decimal。
        channel, unit = record['channel'], record.get('time_unit', 'ms')
        if channel == 'trade':
            return 'trades', self.trade(record, data)
        if channel == 'bookTicker':
            timestamp = micros(data['E'], unit) if 'E' in data else record['local_timestamp']
            return 'book_ticker', dict(zip(BBO, [dec(data[k]) for k in ('A', 'a', 'b', 'B')]), timestamp=timestamp)
        if engine is not None:
            return 'quotes', self.quote(record, data, engine)
        if channel == 'forceOrder':
            order = data['o']
            if order['X'] == 'FILLED':
                # 使用委託價格 p 與累計成交量 z，不能替換為平均成交價或原始委託量。
                return 'liquidations', dict(id=None, side=order['S'].lower(), price=dec(order['p']),
                                            amount=dec(order['z']), timestamp=micros(order['T'], unit))
        if channel in ('ticker', 'markPrice@1s', 'openInterest'):
            return 'derivative_ticker', self.ticker(record, data)
        return None, None

    def consume(self, record):
        # 按原始接收順序重播，將協議解碼、生命週期與各類映射分開處理。
        if record.get('legacy_schema') and record['kind'] in ('message', 'data'):
            kind = 'rest' if record['channel'] in ('depthSnapshot', 'openInterest') else 'ws'
            record = dict(record, kind=kind, generation=-1)
        market, symbol, channel = record['exchange'], record['symbol'], record['channel']
        if market not in TYPES or not symbol:
            return []
        engine = self.depth_engine(record)
        if record['kind'] == 'event':
            self.lifecycle(record, engine)
            return []
        if record['kind'] not in ('ws', 'rest'):
            return []
        try:
            raw = base64.b64decode(record['raw']) if record.get('encoding') == 'base64' else record['raw']
            data = json.loads(raw, parse_float=Decimal)
            if record['kind'] == 'ws':
                if not isinstance(data, dict) or data.get('stream') != f'{symbol.lower()}@{channel}':
                    return []
                data = data['data']
            validate(data, channel, symbol)
            target, row = self.map_payload(record, data, engine)
            if row is not None:
                return [(target, dict(row, exchange=market, symbol=symbol,
                                      local_timestamp=row.get('local_timestamp', record['local_timestamp'])))]
        except BookError as exc:
            self.issues[str(exc)] += 1
        except (ValueError, TypeError, KeyError):
            self.issues['invalid_payload'] += 1
        return []

class Output:
    """依事件 UTC 日期寫 Tardis schema CSV，並追蹤列數與時間範圍。"""
    def __init__(self,root):
        # 建立有限檔案快取、header 紀錄、列數與時間範圍容器。
        self.root=root;self.open=OrderedDict();self.created=set();self.counts=Counter();self.ranges={}
    def ensure(self,market,symbol,day):
        # 為市場／商品／日期建立全部適用的 Tardis 欄位 header，包括零清算情況。
        for target in TYPES[market]:
            rel=f'{market}/{target}/{day}_{symbol}.csv'
            if rel not in self.created:
                path=self.root/rel;path.parent.mkdir(parents=True,exist_ok=True)
                with path.open('x',newline='') as f:csv.writer(f).writerow(SCHEMAS[target])
                self.created.add(rel)
    def write(self,target,row):
        # 以 Decimal 序列化正規化列，更新檔案快取、計數與事件時間範圍。
        market,symbol=row['exchange'],row['symbol'];day=utc(row['timestamp']);self.ensure(market,symbol,day)
        rel=f'{market}/{target}/{day}_{symbol}.csv'
        if rel not in self.open:
            if len(self.open)>=40:self.open.popitem(last=False)[1][0].close()
            f=(self.root/rel).open('a',newline='');self.open[rel]=(f,csv.DictWriter(f,fieldnames=SCHEMAS[target]))
        self.open.move_to_end(rel);self.open[rel][1].writerow({k:encode(row.get(k)) for k in SCHEMAS[target]})
        self.counts[rel]+=1;t=row['timestamp'];low,high=self.ranges.get(rel,(t,t));self.ranges[rel]=(min(low,t),max(high,t))
    def close(self):
        # 關閉全部輸出 handle，確保緩衝資料寫出。
        for f,_ in self.open.values():f.close()

def normalize(source,output,recover=False,start=None,end=None):
    # 驗證來源後離線重播；早期事件仍建立狀態，時間窗只限制最後輸出的列。
    warnings=[]; runs=list(read_runs(source,recover,warnings))
    for _,manifest in runs:
        markets=manifest.get('config',{}).get('markets',[manifest['exchange']] if 'exchange' in manifest else [])
        if not set(markets)<=TYPES.keys():raise ValueError('Tardis normalization currently supports Binance only; keep other markets raw')
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    db=sqlite3.connect(output/'.dedup.sqlite');mapper=Mapper(db);writer=Output(output)
    inputs=[];count=0;report={'status':'running','mapper_reference':reference_metadata()};last_end=None
    atomic_json(output/'normalization_report.json',report)
    LOG.info('event=normalization_started input=%s output=%s runs=%s',source,output,len(runs))
    try:
        for root,manifest in runs:
            if last_end is not None and manifest['started_us']<last_end:
                raise ValueError('overlapping runs require explicit replica merging; unsupported')
            last_end=manifest.get('ended_us',manifest['started_us'])
            mapper.reset();run_count=0
            inputs.append({'run':str(root),'manifest_status':manifest['status'],'health':manifest.get('health_at_stop')})
            for r in read_records(root,recover,warnings):
                if r['exchange'] and r['exchange'] not in TYPES:raise ValueError('Tardis normalization currently supports Binance only')
                count+=1;run_count+=1
                # 零事件也建立欄位標頭，讓安靜清算與缺失來源可以辨識。
                if r['exchange'] in TYPES and r['symbol']:
                    day=utc(r['local_timestamp'])
                    if (start is None or r['local_timestamp']>=start) and (end is None or r['local_timestamp']<end):
                        writer.ensure(r['exchange'],r['symbol'],day)
                for target,row in mapper.consume(r):
                    t=row['timestamp']
                    if (start is None or t>=start) and (end is None or t<end):writer.write(target,row)
                if count%10000==0:db.commit()
                if count%100000==0:LOG.info('event=normalization_progress raw_records=%s csv_rows=%s',count,sum(writer.counts.values()))
            check_count(root,manifest,run_count,recover,warnings)
            LOG.info('event=normalization_run_complete run=%s raw_records=%s',root,run_count)
        report['status']='completed_with_warnings' if warnings else 'complete'
    except BaseException as e:
        report.update(status='failed',error=f'{type(e).__name__}: {e}');raise
    finally:
        writer.close();db.close();(output/'.dedup.sqlite').unlink(missing_ok=True)
        report.update(inputs=inputs,raw_records=count,rows=dict(writer.counts),timestamp_ranges=writer.ranges,
                      mapper_issues=dict(mapper.issues),recovery_warnings=warnings,start_us=start,end_us=end,
                      note='complete means replay completed; consult input health and gaps before claiming coverage.')
        source_issues=any(not i['health'] or i['health'].get('data_quality')!='healthy' for i in inputs)
        mapping_issues=any(v for k,v in mapper.issues.items() if k not in ('off_book_trades','duplicate_trades'))
        report['data_quality']='review_required' if warnings or source_issues or mapping_issues else 'no_detected_issue'
        atomic_json(output/'normalization_report.json',report)
        LOG.info('event=normalization_finished status=%s quality=%s raw_records=%s csv_rows=%s',report['status'],report['data_quality'],count,sum(writer.counts.values()))
    return report

def main():
    # 解析第 02 階段參數與時間窗，執行轉換並輸出機器可讀的結果摘要。
    configure_logging()
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',required=True);p.add_argument('--output',required=True)
    p.add_argument('--recover',action='store_true');p.add_argument('--start-us',type=int);p.add_argument('--end-us',type=int)
    args=p.parse_args()
    if args.start_us is not None and args.end_us is not None and args.start_us>=args.end_us:p.error('invalid interval')
    r=normalize(args.input,args.output,args.recover,args.start_us,args.end_us);print(json.dumps({'status':r['status'],'data_quality':r['data_quality'],'rows':r['rows']}))
if __name__=='__main__':main()
