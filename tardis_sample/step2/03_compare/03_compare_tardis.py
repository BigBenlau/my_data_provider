#!/usr/bin/env python3
"""第 03 階段：在明確的共同時間窗內，比較本地 CSV 與 Tardis CSV；不連線下載。"""
import argparse
from collections import Counter
import csv
from datetime import datetime,timezone
import gzip
import json
from pathlib import Path
import sys
import sqlite3

# 直接以腳本執行時，以腳本位置找到 Step2 共用套件。
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    __package__ = Path(__file__).resolve().parent.name

from common import SCHEMAS,BBO,dec,encode,atomic_json,LOG,configure_logging


def timestamp(value):
    # 接受 epoch 微秒或含時區的 ISO 時間；拒絕模糊的無時區時間。
    if value.isdigit():return int(value)
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('ISO timestamp must include timezone')
    return int(dt.timestamp()*1e6)


def files(root):
    # 尋找標準目錄內的 CSV/gzip，依市場、類型、日期、商品建立索引並拒絕重複來源。
    result={}
    for p in Path(root).glob('*/*/*_*.csv*'):
        if not (p.name.endswith('.csv') or p.name.endswith('.csv.gz')):continue
        market,target=p.parent.parent.name,p.parent.name
        if target not in SCHEMAS:continue
        stem=p.name.removesuffix('.gz').removesuffix('.csv');day,symbol=stem.split('_',1)
        k=(market,target,day,symbol)
        if k in result:raise ValueError(f'duplicate CSV/gzip source for {k}')
        result[k]=p
    return result


def read_csv(path,schema):
    # 串流讀取並核對欄位順序；允許空的清算參考檔。
    opener=gzip.open if path.name.endswith('.gz') else open
    with opener(path,'rt',newline='') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames is None:return  # 允許清算參考檔為零位元組。
        if reader.fieldnames!=schema:raise ValueError(f'schema mismatch: {path}')
        yield from reader


def canonical(row,fields):
    # 把指定欄位轉成可比較表示；數值消除格式差異，ID 與方向保持字串。
    nonnumeric={'exchange','symbol','id','side'}
    return json.dumps([row.get(k,'') if k in nonnumeric or row.get(k,'')=='' else encode(dec(row[k])) for k in fields],separators=(',',':'))


def load(db,path,target,which,start,end):
    # 只載入共同時間窗內的列到磁碟 SQLite，保留重複計數與接收時間。
    count=0;low=high=None
    if path is None:return {'exists':False,'rows':0,'first_us':None,'last_us':None}
    schema=SCHEMAS[target]
    for row in read_csv(path,schema):
        t=int(row['timestamp'])
        if not start<=t<end:continue
        count+=1;low=t if low is None else min(low,t);high=t if high is None else max(high,t)
        if target=='trades':
            if not row['id']:raise ValueError('trade row missing id')
            k=row['id'];value=canonical(row,['timestamp','side','price','amount']);state=None
        else:
            value=canonical(row,[k for k in schema if k not in ('local_timestamp','exchange','symbol')]);k=value
            state=canonical(row,BBO) if target in ('quotes','book_ticker') else None
        db.execute(f'INSERT INTO {which} VALUES (?,?,?,?,?) ON CONFLICT(k,v) DO UPDATE SET n=n+1,local=min(local,excluded.local)',
                   (k,value,1,int(row['local_timestamp']),state))
        if count%10000==0:db.commit()
    db.commit();return {'exists':True,'rows':count,'first_us':low,'last_us':high}


def compare_trades(db, row):
    # 成交以 ID 比較覆蓋與內容；本機接收時間差只作觀察，不等同純網路延遲。
    for table in ('ours','tardis'):db.execute(f'CREATE INDEX IF NOT EXISTS {table}_key ON {table}(k)')
    shared=db.execute('SELECT count(*) FROM (SELECT DISTINCT k FROM ours INTERSECT SELECT DISTINCT k FROM tardis)').fetchone()[0]
    only_a=db.execute('SELECT count(*) FROM (SELECT DISTINCT k FROM ours EXCEPT SELECT DISTINCT k FROM tardis)').fetchone()[0]
    only_b=db.execute('SELECT count(*) FROM (SELECT DISTINCT k FROM tardis EXCEPT SELECT DISTINCT k FROM ours)').fetchone()[0]
    same=db.execute('SELECT count(*) FROM (SELECT DISTINCT a.k FROM ours a JOIN tardis b ON a.k=b.k AND a.v=b.v)').fetchone()[0]
    mismatches=db.execute('SELECT DISTINCT a.k FROM ours a JOIN tardis b ON a.k=b.k WHERE NOT EXISTS(SELECT 1 FROM tardis c WHERE c.k=a.k AND c.v=a.v) LIMIT 20').fetchall()
    delta=db.execute('SELECT min(a.local-b.local),avg(a.local-b.local),max(a.local-b.local) FROM ours a JOIN tardis b ON a.k=b.k AND a.v=b.v').fetchone()
    row.update(shared_trade_ids=shared,only_ours_ids=only_a,only_tardis_ids=only_b,
               ids_with_matching_content=same,mismatch_id_examples=[x[0] for x in mismatches],
               local_time_difference_us={'min':delta[0],'mean':delta[1],'max':delta[2]},
               local_time_note='Ours minus Tardis receive time; includes host clock/path differences, not pure network latency.')
    row['tardis_id_coverage']=shared/(shared+only_b) if shared+only_b else None
    row['ours_id_coverage']=shared/(shared+only_a) if shared+only_a else None
    for table in ('ours','tardis'):
        row[table]['ids_with_conflicting_content']=db.execute(f'SELECT count(*) FROM (SELECT k FROM {table} GROUP BY k HAVING count(*)>1)').fetchone()[0]


def compare_events(db, row):
    # 非成交資料保留重複次數比較；最佳價另計狀態覆蓋，避免誤判接收時間差。
    a, b = row['ours'], row['tardis']
    target, market = row['type'], row['exchange']
    matches=db.execute('SELECT coalesce(sum(min(a.n,b.n)),0) FROM ours a JOIN tardis b ON a.k=b.k AND a.v=b.v').fetchone()[0]
    row.update(exact_event_rows_matched=matches,unmatched_ours_rows=a['rows']-matches,unmatched_tardis_rows=b['rows']-matches,
               interpretation='Exact event timestamp+values excluding local_timestamp; differing publishing/snapshot/OI cadence is not proof of loss.')
    if target in ('quotes','book_ticker'):
        states=db.execute('SELECT count(*) FROM (SELECT DISTINCT state FROM ours INTERSECT SELECT DISTINCT state FROM tardis)').fetchone()[0]
        row['shared_bbo_states']=states
        row['ours_unique_bbo_states']=db.execute('SELECT count(DISTINCT state) FROM ours').fetchone()[0]
        row['tardis_unique_bbo_states']=db.execute('SELECT count(DISTINCT state) FROM tardis').fetchone()[0]
        if market=='binance' and target=='book_ticker':row['timestamp_note']='Spot bookTicker has no E; timestamp falls back to each collector local time. Prefer state coverage.'


def compare_pair(db, identity, ours, reference, start, end):
    # 每個商品、日期與資料類型獨立載入，缺少參考檔或空窗都明確標示。
    market, target, day, symbol = identity
    for table in ('ours', 'tardis'):
        db.execute(f'DROP TABLE IF EXISTS {table}')
        db.execute(f'CREATE TABLE {table}(k TEXT,v TEXT,n INTEGER,local INTEGER,state TEXT,PRIMARY KEY(k,v)) WITHOUT ROWID')
    a = load(db, ours, target, 'ours', start, end)
    b = load(db, reference, target, 'tardis', start, end)
    row = dict(exchange=market, symbol=symbol, type=target, date=day, ours=a, tardis=b)
    if target == 'trades':
        compare_trades(db, row)
    else:
        compare_events(db, row)
    for table in ('ours', 'tardis'):
        row[table]['duplicate_identical_rows'] = db.execute(f'SELECT coalesce(sum(n-1),0) FROM {table}').fetchone()[0]
    row['assessment'] = 'missing_reference' if not b['exists'] else ('empty_window' if not a['rows'] or not b['rows'] else 'compared')
    LOG.info('event=comparison_file_complete market=%s type=%s symbol=%s date=%s assessment=%s ours_rows=%s reference_rows=%s',
             market, target, symbol, day, row['assessment'], a['rows'], b['rows'])
    return row


def compare(ours, tardis, output, start, end):
    # 只比較使用者明確指定的時間窗；逐檔重用磁碟資料庫，限制記憶體用量。
    if start >= end:
        raise ValueError('invalid comparison interval')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    own, ref = files(ours), files(tardis)
    if not own:
        raise ValueError('no collected CSV files')
    db = sqlite3.connect(output / '.compare.sqlite')
    results = []
    LOG.info('event=comparison_started ours=%s tardis=%s start_us=%s end_us=%s', ours, tardis, start, end)
    try:
        for identity, path in sorted(own.items()):
            day = identity[2]
            day_start = int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1e6)
            if day_start >= end or day_start + 86400_000_000 <= start:
                continue
            results.append(compare_pair(db, identity, path, ref.get(identity), start, end))
        report = dict(start_us=start, end_us_exclusive=end, results=results,
                      note='Explicit requested window. Empty/missing data is reported, never treated as perfect agreement. No historical data is downloaded.')
        atomic_json(output / 'comparison.json', report)
        LOG.info('event=comparison_finished files=%s output=%s', len(results), output)
        return report
    finally:
        db.close()
        (output / '.compare.sqlite').unlink(missing_ok=True)

def main():
    # 解析第 03 階段的兩份資料與共同時間窗，輸出對帳報告位置。
    configure_logging()
    p=argparse.ArgumentParser(description=__doc__)
    for k in ('ours','tardis','output','start','end'):p.add_argument('--'+k,required=True)
    a=p.parse_args();r=compare(a.ours,a.tardis,a.output,timestamp(a.start),timestamp(a.end))
    print(json.dumps({'comparisons':len(r['results']),'report':str(Path(a.output)/'comparison.json')}))
if __name__=='__main__':main()
