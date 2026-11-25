"""Full-day empirical analysis; invoked by analyze_dataset_csv.py --empirical.

Dependency: duckdb. Each source is read in full; disk databases and capped memory
avoid holding a day's BBO stream in Python memory. JSON/CSV artifacts are internal.

Recompute internal statistics from the workspace root (delivery reports are edited separately):
  python3 -m pip install -r tardis_sample/step1/requirements-analysis.txt
  python3 tardis_sample/step1/analyze_dataset_csv.py --empirical \
    --source tardis_sample/datasets --output tardis_sample/step1/research_results \
    --date 2026-09-18

The empirical mode has a fixed, strict 5-second threshold. Source read/CSV errors
abort the run with their original exception, rather than yielding empty data.
"""
import argparse
import csv
import gzip
import itertools
import json
import math
import re
from array import array
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import shutil
from contextlib import contextmanager
import duckdb

FIELDS = ['funding_timestamp', 'funding_rate', 'predicted_funding_rate',
          'open_interest', 'last_price', 'index_price', 'mark_price']
BBO = ['bid_price', 'bid_amount', 'ask_price', 'ask_amount']
ASSETS = ['BTC', 'ETH', 'SOL', 'GRT']


def qi(s):
    return '"' + s.replace('"', '""') + '"'


def qs(s):
    return "'" + str(s).replace("'", "''") + "'"


def utc(t):
    return datetime.fromtimestamp(t // 1000000, timezone.utc).replace(microsecond=t % 1000000).isoformat()


def connect(path, scratch):
    con = duckdb.connect(str(path))
    con.execute("SET memory_limit='512MB'")
    con.execute('SET threads=2')
    con.execute("SET preserve_insertion_order=true")
    con.execute('SET temp_directory=' + qs(scratch))
    return con


def reader(path):
    # Single-thread scan preserves original CSV record order for row_number().
    return f"read_csv({qs(path)}, header=true, all_varchar=true, parallel=false, nullstr='', strict_mode=true)"


def batches(cursor, size=16384):
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            return
        yield from rows


def nearest(values, p):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)]


def overlap(a, b, incidents):
    hits = [i for i in incidents if a < i['end'] and b > i['start']]
    intervals = sorted((max(a, i['start']), min(b, i['end'])) for i in hits)
    total = 0
    end = None
    for x, y in intervals:
        total += max(0, y - max(x, end if end is not None else x))
        end = max(y, end if end is not None else y)
    return len(hits), total, '; '.join(i['details'] for i in hits)


def intervals(con, table='times'):
    return batches(con.execute(f'''SELECT t, next_t FROM (
      SELECT t, lead(t) OVER (ORDER BY t) next_t FROM (SELECT DISTINCT t FROM {table} WHERE t IS NOT NULL)
    ) WHERE next_t-t>5000000 ORDER BY t'''))


def required(ex):
    types = {'trades', 'quotes', 'book_ticker'}
    if ex in ('binance-futures', 'bybit', 'okex-swap'):
        types |= {'derivative_ticker', 'liquidations'}
    elif ex == 'upbit':
        types.remove('book_ticker')
    return types


def inspect_file(path, date, incidents, gap_writer, work, profile):
    name = path.name.removesuffix('.gz')
    ex, kind = path.parent.parent.name, path.parent.name
    symbol = name[11:-4]
    key = f'{ex}/{kind}/{name}'
    opener = gzip.open if path.suffix == '.gz' else open
    # A complete read of an empty gzip validates its trailer/CRC.
    with opener(path, 'rt', newline='') as stream:
        header = next(csv.reader(stream), None)
    result = dict(exchange=ex, symbol=symbol, kind=kind, date=date, file=key,
                  rows=0, status='empty_file', fields={}, gaps=0, max_gap_us=None,
                  incident_matches=0, backwards=0, changes={}, min_us=None, max_us=None)
    day = int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp()) * 1000000
    if header is None:
        result['all_day_no_records'] = True
        profile.write(json.dumps(result)+'\n');profile.flush()
        return result, []
    if not header or any(not h for h in header) or len(set(header)) != len(header):
        raise ValueError(f'Invalid header: {path}')
    if 'timestamp' not in header or 'local_timestamp' not in header:
        raise ValueError(f'Missing time fields: {path}')
    con = connect(work/'file.duckdb', work/'sort')
    try:
        con.execute(f'CREATE TABLE raw AS SELECT row_number() OVER () r, * FROM {reader(path)}')
        n = con.execute('SELECT count(*) FROM raw').fetchone()[0]
        result.update(rows=n, status='ok' if n else 'header_only')
        nulls = con.execute('SELECT '+', '.join(f"count(*) FILTER (WHERE {qi(h)} IS NULL OR trim({qi(h)})='')" for h in header)+' FROM raw').fetchone()
        result['fields'] = {h: dict(empty=v, filled=n-v) for h,v in zip(header,nulls)}
        con.execute('CREATE TABLE times AS SELECT r, try_cast(timestamp AS BIGINT) t, try_cast(local_timestamp AS BIGINT) l FROM raw')
        invalid = con.execute('SELECT count(*) FILTER (WHERE t IS NULL), count(*) FILTER (WHERE l IS NULL) FROM times').fetchone()
        result['invalid_times'] = list(invalid)
        if any(invalid):
            raise ValueError(f'Missing or invalid time: {path}, {invalid}')
        lo, hi = con.execute('SELECT min(t),max(t) FROM times').fetchone()
        result.update(min_us=lo,max_us=hi)
        result['backwards'] = con.execute('SELECT count(*) FROM (SELECT t,lag(t) OVER(ORDER BY r) prev FROM times) WHERE t<prev').fetchone()[0]
        if n:
            day_lo,day_hi=con.execute('SELECT min(t),max(t) FROM times WHERE t>=? AND t<?',[day,day+86400000000]).fetchone()
            result.update(day_start_unobserved_us=day_lo-day if day_lo is not None else None,
                          day_end_unobserved_us=day+86400000000-day_hi if day_hi is not None else None)
        else:
            result['all_day_no_records'] = True
        # Gap endpoints also reconstruct compressed activity intervals for the union.
        coverage = []
        start = lo
        for a,b in intervals(con):
            coverage.append((start,a+5000000))
            start = b
            if not day <= a < b < day+86400000000:
                result['boundary_gap_count']=result.get('boundary_gap_count',0)+1
                continue
            hit, micros, details = overlap(a,b,incidents)
            gap_writer.writerow([ex,symbol,kind,date,a,b,utc(a),utc(b),(b-a)/1e6,hit,micros/1e6,details])
            result['gaps'] += 1
            result['max_gap_us'] = max(result['max_gap_us'] or 0,b-a)
            result['incident_matches'] += bool(hit)
        if start is not None:
            coverage.append((start,hi+5000000))
        if kind == 'derivative_ticker' and n:
            for field in FIELDS:
                f=qi(field)
                # Finite exact decimals, no value change across null/initialization.
                con.execute(f'''CREATE OR REPLACE TEMP TABLE deltas AS
                WITH v AS (SELECT r,try_cast(local_timestamp AS BIGINT) t,
                  CAST({f} AS DECIMAL(38,18)) v FROM raw),
                marked AS (SELECT *,lag(v) OVER(ORDER BY r) pv,
                  sum(CASE WHEN v IS NULL THEN 1 ELSE 0 END) OVER(ORDER BY r) segment FROM v),
                changes AS (SELECT r,t,segment FROM marked WHERE v IS NOT NULL AND pv IS NOT NULL AND v<>pv)
                SELECT *,t-lag(t) OVER(PARTITION BY segment ORDER BY r) delta FROM changes''')
                count, negative = con.execute('SELECT count(*),count(*) FILTER(WHERE delta<0) FROM deltas').fetchone()
                if negative: raise ValueError('Nonmonotonic arrival times in field changes')
                quantiles=con.execute('SELECT quantile_disc(delta,0.5),quantile_disc(delta,0.95),count(delta) FROM deltas').fetchone()
                result['changes'][field] = dict(count=count,median_us=quantiles[0],p95_us=quantiles[1],intervals=quantiles[2])
        profile.write(json.dumps(result)+'\n');profile.flush()
        return result,coverage
    finally:
        con.close()
        (work/'file.duckdb').unlink(missing_ok=True)
        (work/'file.duckdb.wal').unlink(missing_ok=True)


def pair_state(left,right,window):
    """Greedy chronological, symmetric FIFO. Candidate counts include all events,
    even ones matched earlier: uniqueness means isolation in the original window.
    Each tuple is (arrival microseconds, CSV record number).
    """
    import bisect
    lt=[v[0] for v in left];rt=[v[0] for v in right]
    i=j=0
    while i<len(left) and j<len(right):
        a,b=lt[i],rt[j]
        if a < b-window:i+=1
        elif b < a-window:j+=1
        else:
            nc=bisect.bisect_right(rt,a+window)-bisect.bisect_left(rt,a-window)
            mc=bisect.bisect_right(lt,b+window)-bisect.bisect_left(lt,b-window)
            yield b-a,nc==mc==1,left[i],right[j]
            i+=1;j+=1


@contextmanager
def bbo_workspace(output, asset, signature):
    path=Path(output)/('_bbo_work_'+asset)
    path.mkdir(exist_ok=True)
    stamp=path/'inputs.json'
    if stamp.exists():
        if json.loads(stamp.read_text()) != signature:
            raise ValueError(f'Checkpoint inputs changed: {path}; remove this workspace before rerunning')
    elif any(path.iterdir()):
        raise ValueError(f'Unverified checkpoint: {path}; remove this workspace before rerunning')
    else:
        stamp.write_text(json.dumps(signature))
    yield str(path)
    shutil.rmtree(path)


def time_disagreement(events, end):
    """Integer states, receive-time sorted; equal-time updates consume zero time."""
    current=[None,None];previous=None
    totals=[0,0,0,0,0]
    def integrate(stop):
        if previous is None or current[0] is None or current[1] is None:return
        dt=min(stop,end)-previous
        if dt<=0:return
        b,q=current
        bid=b[0]!=q[0];ask=b[2]!=q[2]
        totals[0]+=dt;totals[1]+=dt*bid;totals[2]+=dt*ask
        totals[3]+=dt*(bid or ask)
        totals[4]+=dt*(not bid and not ask and (b[1]!=q[1] or b[3]!=q[3]))
    for event in events:
        t,src,*state=event
        integrate(t)
        if t>end:
            previous=end
            break
        current[src]=state;previous=t
    integrate(end)
    return dict(zip(['observed_us','bid_us','ask_us','any_price_us','amount_only_us'],totals))


def bbo_analysis(source,asset,date,output):
    print('BBO',asset,'loading',flush=True)
    signature={'version':2,'date':date,'files':[]}
    for kind in ['book_ticker','quotes']:
        path=source/'binance-futures'/kind/f'{date}_{asset}USDT.csv'
        if not path.exists():path=path.with_suffix('.csv.gz')
        stat=path.stat();signature['files'].append([str(path.resolve()),stat.st_size,stat.st_mtime_ns])
    with bbo_workspace(output,asset,signature) as tmp:
        work=Path(tmp)
        if not (work/'prepared.json').exists():
            (work/'bbo.duckdb').unlink(missing_ok=True)
            (work/'bbo.duckdb.wal').unlink(missing_ok=True)
            shutil.rmtree(work/'sort',ignore_errors=True)
        con=connect(work/'bbo.duckdb',work/'sort')
        con.execute("SET memory_limit='2GB'")
        con.execute('SET threads=1')
        if (work/'prepared.json').exists():
            prepared=json.loads((work/'prepared.json').read_text())
            result=prepared['result'];bounds={int(k):v for k,v in prepared['bounds'].items()}
        else:
            counts={}
            bounds={}
            for src,kind in enumerate(['book_ticker','quotes']):
                path=source/'binance-futures'/kind/f'{date}_{asset}USDT.csv'
                if not path.exists():path=path.with_suffix('.csv.gz')
                # Validate exact decimal grammar in the same scan; reject precision loss.
                dec=', '.join(f"CASE WHEN regexp_full_match({qi(f)}, '[+-]?[0-9]+([.][0-9]{{0,8}}0*)?') THEN CAST({qi(f)} AS DECIMAL(18,8)) ELSE error('Invalid or inexact BBO decimal') END {qi(f)}" for f in BBO)
                con.execute('SET preserve_insertion_order=true')
                con.execute(f'''CREATE TABLE raw AS SELECT row_number() OVER() r,
                 CAST(local_timestamp AS BIGINT) t, {dec} FROM {reader(path)}''')
                n,lo,hi=con.execute('SELECT count(*),min(t),max(t) FROM raw').fetchone()
                con.execute('SET preserve_insertion_order=false')
                counts[kind]=n
                bounds[src]=(lo,hi)
                if con.execute('SELECT count(*) FROM raw WHERE t IS NULL').fetchone()[0]:
                    raise ValueError('Missing BBO arrival time')
                if con.execute('SELECT count(*) FROM (SELECT t,lag(t) OVER(ORDER BY r) p FROM raw) WHERE t<p').fetchone()[0]:
                    raise ValueError('BBO arrival order is nonmonotonic; external stable sort required')
                conditions=' OR '.join(f'{qi(f)} IS DISTINCT FROM lag({qi(f)}) OVER(ORDER BY r)' for f in BBO)
                con.execute(f'''CREATE TABLE s{src} AS SELECT r,t,[{','.join(BBO)}]::DECIMAL(18,8)[4] state
                  FROM raw QUALIFY r=1 OR {conditions}''')
                con.execute('DROP TABLE raw');con.execute('CHECKPOINT')
            con.execute('CREATE TABLE occurrences AS SELECT 0 src,* FROM s0 UNION ALL SELECT 1 src,* FROM s1')
            con.execute('CREATE TABLE states AS SELECT state, bool_or(src=0) b,bool_or(src=1) q FROM occurrences GROUP BY state')
            unique=con.execute('SELECT count(*) FILTER(WHERE b),count(*) FILTER(WHERE q),count(*) FILTER(WHERE b AND q),count(*) FILTER(WHERE b AND NOT q),count(*) FILTER(WHERE q AND NOT b) FROM states').fetchone()
            occ=con.execute('SELECT src,count(*) FROM occurrences GROUP BY src ORDER BY src').fetchall()
            result=dict(asset=asset,rows=counts,occurrences=dict(occ),distinct=dict(zip(['book','quotes','shared','book_only','quote_only'],unique)),windows={})
            con.execute('CHECKPOINT')
            (work/'prepared.json').write_text(json.dumps(dict(result=result,bounds=bounds)))
        # Arrival-state disagreement: one row per source/timestamp, last record wins.
        print('BBO',asset,'disagreement',flush=True)
        end=min(bounds[0][1],bounds[1][1])
        if (work/'disagreement.json').exists():
            result['disagreement']=json.loads((work/'disagreement.json').read_text())
        else:
            columns=','.join(f'CAST(CAST(state[{i}] AS DECIMAL(38,8))*100000000 AS BIGINT)' for i in range(1,5))
            event_cursor=con.cursor().execute(f'SELECT t,src,{columns} FROM occurrences ORDER BY t,src,r')
            result['disagreement']=time_disagreement(batches(event_cursor),end)
            event_cursor.close()
            (work/'disagreement.json').write_text(json.dumps(result['disagreement']))
        print('BBO',asset,'state matching',flush=True)
        # Disk sort groups state occurrences. Only one state is held in Python.
        cursor=con.cursor().execute('SELECT o.state,o.src,o.t,o.r FROM occurrences o JOIN states s ON o.state=s.state WHERE s.b AND s.q ORDER BY o.state,o.t,o.src,o.r')
        accum={w:{'all':array('q'),'unique':array('q'),'examples':[]} for w in [100000,1000000,5000000]}
        for state,group in itertools.groupby(batches(cursor),key=lambda r:r[0]):
            left=[];right=[]
            for _,src,t,r in group:(left if src==0 else right).append((t,r))
            if not left or not right:continue
            for window,stats in accum.items():
                for delta,unique_pair,l,r in pair_state(left,right,window):
                    stats['all'].append(delta)
                    if unique_pair:stats['unique'].append(delta)
                    if window==1000000 and unique_pair and len(stats['examples'])<8:
                        stats['examples'].append(dict(state=list(map(str,state)),book_time=l[0],quote_time=r[0],book_record=l[1],quote_record=r[1],delta_us=delta))
        cursor.close()
        for window,stats in accum.items():
            output_stats={}
            for label in ['all','unique']:
                ds=stats[label]
                output_stats[label]=dict(n=len(ds),book_first=sum(d>0 for d in ds),quotes_first=sum(d<0 for d in ds),same=sum(d==0 for d in ds),median_us=nearest(ds,.5),p95_us=nearest(ds,.95),abs_p95_us=nearest(array('q',(abs(d) for d in ds)),.95))
            output_stats['examples']=stats['examples']
            result['windows'][str(window)]=output_stats
        con.close()
        return result


def symbol_report(results, common):
    lines=[f"# {common['exchange']} / {common['symbol']}", '',
           '| Type | Status | Rows | First event UTC | Last event UTC | Gaps >5s | Max gap s | Reversals |',
           '| --- | --- | --- | --- | --- | --- | --- | --- |']
    for r in results:
        lines.append('| '+' | '.join(map(str,[r['kind'],r['status'],r['rows'],
            utc(r['min_us']) if r['min_us'] is not None else '—',
            utc(r['max_us']) if r['max_us'] is not None else '—',r['gaps'],
            r['max_gap_us']/1e6 if r['max_gap_us'] is not None else '—',r['backwards']]))+' |')
    lines += ['', f"Common silence intervals: {common['count']}; max seconds: {common['max_us']/1e6}.", '',
              '| Field | Value changes | Median interval s | p95 interval s |', '| --- | --- | --- | --- |']
    for r in results:
        for field,c in r['changes'].items():
            lines.append('| '+' | '.join(map(str,[field,c['count'],c['median_us']/1e6 if c['median_us'] is not None else '—',c['p95_us']/1e6 if c['p95_us'] is not None else '—']))+' |')
    return '\n'.join(lines)+'\n'


def common_intervals(coverage):
    """Coverage is a union of [event, event+5s] segments; empty feeds add none."""
    end=None
    for a,b in sorted(coverage):
        if end is not None and a>end:
            yield end-5000000,a
        end=max(end,b) if end is not None else b


def run(source,output,date):
    source=Path(source).resolve();output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
    files=sorted(p for p in source.rglob('*') if p.is_file() and p.name.startswith(date+'_')
                 and (p.name.endswith('.csv') or p.name.endswith('.csv.gz'))
                 and not re.search(r'(^|[_\-.])sample([_\-.]|$)',p.name,re.IGNORECASE))
    files=[p for p in files if not(p.suffix=='.gz' and p.with_suffix('') in files)]
    groups=defaultdict(list)
    for p in files:groups[p.parent.parent.name,p.name.removesuffix('.gz')[11:-4]].append(p)
    all_results=[];common=[]
    with (output/'file_gaps.csv').open('w') as fg,(output/'common_gaps.csv').open('w') as cg,(output/'files.jsonl').open('w') as profile:
        fw=csv.writer(fg);cw=csv.writer(cg)
        fw.writerow(['exchange','symbol','kind','date','start_us','end_us','start_utc','end_utc','seconds','incident_count','overlap_seconds','incident_details'])
        cw.writerow(['exchange','symbol','date','start_us','end_us','seconds','incident_count'])
        for (ex,sym),paths in sorted(groups.items()):
            metadata=json.loads((Path(__file__).resolve().parent.parent/'exchange_info'/f'{ex}.json').read_text())
            incidents=[dict(start=int(datetime.fromisoformat(i['from'].replace('Z','+00:00')).timestamp()*1e6),end=int(datetime.fromisoformat(i['to'].replace('Z','+00:00')).timestamp()*1e6),details=i['details']) for i in metadata['incidentReports']]
            results=[];coverage=[]
            with tempfile.TemporaryDirectory(prefix='profile-',dir=output) as tmp:
                for p in paths:
                    print('PROFILE',ex,sym,p.parent.name,flush=True)
                    r,c=inspect_file(p,date,incidents,fw,Path(tmp),profile)
                    results.append(r)
                    if p.parent.name in required(ex):coverage.extend(c)
            missing=required(ex)-{r['kind'] for r in results}
            if missing:raise ValueError(f'Missing types for {ex}/{sym}: {missing}')
            n=0;maximum=0;hits=0
            for start,a in common_intervals(coverage):
                day=int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp())*1000000
                if not day <= start < a < day+86400000000:continue
                hit,_,_=overlap(start,a,incidents)
                cw.writerow([ex,sym,date,start,a,(a-start)/1e6,hit]);n+=1;maximum=max(maximum,a-start);hits+=bool(hit)
            summary=dict(exchange=ex,symbol=sym,count=n,max_us=maximum,incident_matches=hits)
            common.append(summary);all_results.extend(results)
            destination=output/ex;destination.mkdir(exist_ok=True)
            (destination/f'{sym}.json').write_text(json.dumps(dict(files=results,common=summary),indent=2))
            (destination/f'{sym}.md').write_text(symbol_report(results,summary))
            fg.flush();cg.flush()
            print('SAVED',ex,sym,flush=True)
    result=dict(date=date,files=all_results,common=common,bbo=[])
    (output/'empirical.json').write_text(json.dumps(result,indent=2))
    for asset in ASSETS:
        b=bbo_analysis(source,asset,date,output);result['bbo'].append(b)
        (output/f'bbo_{asset}.json').write_text(json.dumps(b,indent=2))
        (output/'empirical.json').write_text(json.dumps(result,indent=2))
        print('BBO DONE',asset,flush=True)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,default=Path(__file__).resolve().parent.parent/'datasets_extracted');parser.add_argument('--output',type=Path,default=Path(__file__).resolve().parent/'research_results'/'empirical');parser.add_argument('--date',default='2026-09-18')
    a=parser.parse_args();run(a.source,a.output,a.date)
