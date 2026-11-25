"""串流重播三版原始紀錄，驗證序號、提交範圍及採集鎖。"""
import fcntl
import gzip
import json
from pathlib import Path


def open_segment(path):
    # 壓縮只改儲存方式，不改 JSON 行、提交序號與原生時間。
    return gzip.open(path, 'rt', encoding='utf8') if path.suffix == '.gz' else path.open(encoding='utf8')


def read_records(run,recover=False,warnings=None):
    # 串流讀取 raw；驗證 seq，舊檔按 seq 合併，.part 僅在明確 recover 時讀完整行。
    warnings=warnings if warnings is not None else []
    manifest = Path(run) / 'manifest.json'
    if manifest.exists() and json.loads(manifest.read_text()).get('schema_version') == 3:
        yield from read_merged_records(run, recover, warnings)
        return
    expected=1
    files=sorted((Path(run)/'raw').glob('*.jsonl.gz*'))
    legacy=sorted((Path(run)/'raw').glob('*.jsonl'))
    if legacy:
        if files: raise ValueError('mixed legacy and v2 segments')
        from contextlib import ExitStack
        import heapq
        with ExitStack() as stack:
            def lines(handle):
                # 讀取單個舊版檔案並檢查其內部序號嚴格遞增。
                previous=0
                for line in handle:
                    r=json.loads(line)
                    if r['seq']<=previous: raise ValueError('non-increasing legacy segment sequence')
                    previous=r['seq']; yield dict(r,legacy_schema=True)
            streams=[lines(stack.enter_context(p.open(encoding='utf8'))) for p in legacy]
            for r in heapq.merge(*streams,key=lambda r:r['seq']):
                if r['seq']!=expected: raise ValueError('legacy raw sequence gap/duplicate')
                expected+=1; yield r
        return
    for path in files:
        partial=path.name.endswith('.part')
        if partial and not recover:raise ValueError(f'unclean segment {path}; use --recover')
        try:
            with gzip.open(path,'rt',encoding='utf8') as f:
                for line in f:
                    if not line.endswith('\n'):
                        if partial and recover:
                            warnings.append({'path':str(path),'reason':'incomplete last line'});break
                        raise ValueError('incomplete JSON line')
                    r=json.loads(line)
                    if r['seq']!=expected:
                        if not recover:raise ValueError(f'sequence mismatch: {expected} vs {r["seq"]}')
                        if r['seq']<expected:raise ValueError('duplicate/reversed sequence')
                        warnings.append({'path':str(path),'reason':'sequence gap','expected':expected,'actual':r['seq']})
                    expected=r['seq']+1;yield r
        except (EOFError,OSError) as e:
            if not (partial and recover):raise
            warnings.append({'path':str(path),'reason':str(e)})




def read_merged_records(run, recover=False, warnings=None):
    # 第三版以提交偏移識別有效尾端；不把未提交的完整 JSON 行誤當正式資料。
    expected = 1; root = Path(run); committed = None
    if recover and (root / '_state' / 'merge.sqlite').exists():
        import sqlite3
        db = sqlite3.connect(f'file:{root / "_state" / "merge.sqlite"}?mode=ro', uri=True)
        committed = db.execute('SELECT written FROM output').fetchone()[0]; db.close()
    paths = {p.name.removesuffix('.gz'): p for p in (root / 'raw').glob('*.jsonl.gz')}
    paths.update({p.name: p for p in (root / 'raw').glob('*.jsonl')})
    paths.update({p.name: p for p in (root / 'raw').glob('*.jsonl.part')})
    for _, path in sorted(paths.items()):
        partial = path.name.endswith('.part')
        if partial and not recover: raise ValueError('unclean merged segment; restart supervisor or use --recover')
        with open_segment(path) as handle:
            for line in handle:
                if partial and committed is not None and expected > committed: break
                if not line.endswith('\n'): raise ValueError('incomplete committed JSONL')
                record = json.loads(line)
                if record['seq'] != expected: raise ValueError('merged raw sequence gap/duplicate')
                expected += 1; yield record


def read_runs(source, recover=False, warnings=None):
    # 共用 manifest/schema/程序鎖檢查，拒絕處理仍在採集的 run。
    source=Path(source); warnings=warnings if warnings is not None else []
    roots=[source] if (source/'manifest.json').exists() else sorted(p.parent for p in source.glob('*/manifest.json'))
    if not roots: raise ValueError('no collector runs found')
    for root in roots:
        manifest=json.loads((root/'manifest.json').read_text())
        if manifest.get('schema_version') not in (2, 3) and manifest.get('raw_schema_version')!=1:
            raise ValueError(f'unsupported raw schema: {root}')
        if manifest['status']=='running':
            lock_path = root / '.collector.lock' if manifest.get('schema_version') == 3 else root.parent / '.collector.lock'
            with lock_path.open('a') as lock:
                try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError: raise ValueError('stop the collector before processing this run')
            if not recover: raise ValueError('unclean run requires --recover')
            warnings.append({'run':str(root),'reason':'unclean process exit'})
        yield root,manifest

def check_count(root, manifest, count, recover, warnings):
    # 核對 manifest 與實際 raw 筆數；recover 模式留下警告而不是假裝完整。
    if manifest['status']!='running' and count!=manifest['raw_records']:
        if not recover: raise ValueError('manifest/raw record count mismatch')
        warnings.append({'run':str(root),'reason':'record_count_mismatch','expected':manifest['raw_records'],'actual':count})
