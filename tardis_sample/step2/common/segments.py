"""跨盤封段的位置、裝置身分及重播校驗。"""
from contextlib import closing
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3


def volume(root, binding):
    # 熱盤、錯誤的掛載點或缺少標識的目錄一律不可用作冷盤。
    path = Path(os.environ.get('COLD_ROOT') or binding['root'])
    if path.stat().st_dev == Path(root).stat().st_dev:
        raise OSError(f'cold and hot storage share device: {path}')
    if (path/'.collector-volume-id').read_text().strip() != binding['volume_id']:
        raise OSError(f'wrong cold volume: {path}')
    return path / binding['directory']


def checksum(path):
    # 檢查壓縮原檔的位元組，連最後一段的截斷也能識別。
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda:handle.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def location(root, name):
    # 同一位置查詢供 01 與 02 使用，缺盤時回報受影響的正式序號。
    root = Path(root).resolve()
    path = root/'raw'/name
    if path.is_file():
        return path
    zipped = path.with_suffix('.jsonl.gz')
    if zipped.is_file():
        index = root/'_state/merge.sqlite'
        if index.is_file():
            with closing(sqlite3.connect(index.as_uri()+'?mode=ro',uri=True)) as db:
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='sealed_at'").fetchone():
                    row = db.execute('SELECT first_seq,last_seq,digest FROM sealed_at JOIN segments USING(name) WHERE name=?',(name,)).fetchone()
                    try:
                        if row and row[2] and checksum(zipped)!=row[2]:
                            raise OSError(f'hot segment SHA-256 mismatch {name} seq={row[0]}..{row[1]}')
                    except FileNotFoundError:
                        pass
        if zipped.is_file():
            return zipped
    catalog = root/'_state/cold_catalog.sqlite'
    if not catalog.is_file():
        raise FileNotFoundError(path)
    with closing(sqlite3.connect(catalog.as_uri()+'?mode=ro',uri=True)) as db:
        row = db.execute('SELECT first,last,digest FROM files WHERE name=?',(name,)).fetchone()
    if not row:
        raise FileNotFoundError(path)
    try:
        binding = json.loads((root/'_state/cold.json').read_text())
        target = volume(root,binding)/'raw'/(name+'.gz')
        if checksum(target) != row[2]:
            raise OSError('SHA-256 mismatch')
        return target
    except (OSError,ValueError) as exc:
        raise OSError(f'cold segment {name} seq={row[0]}..{row[1]}: {exc}') from exc


def cold_names(root):
    # 保留精簡熱盤目錄，即使最後的行情段已全部搬離也能列舉。
    catalog = Path(root).resolve()/'_state/cold_catalog.sqlite'
    if catalog.is_file():
        with closing(sqlite3.connect(catalog.as_uri()+'?mode=ro',uri=True)) as db:
            return {r[0] for r in db.execute('SELECT name FROM files')}
    return set()

def sealed_names(root):
    # 停止的批次依目錄檢查所有已提交段，包含尾段遺失或全部已搬離的情形。
    root = Path(root).resolve()
    names = cold_names(root)
    path = root/'_state/merge.sqlite'
    if path.is_file():
        try:
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='segments'").fetchone():
                    names.update(row[0] for row in db.execute('SELECT name FROM segments'))
        except sqlite3.OperationalError:
            if (root/'_state/cold.json').exists():
                raise
    return names


def disk_names(root, pattern):
    # 舊版目錄與新的每日目錄可共存，名稱一律相對於 raw。
    raw = Path(root) / 'raw'
    for layout in (pattern, '????-??-??/' + pattern):
        for path in raw.glob(layout):
            yield path.relative_to(raw).as_posix()


def first_seq(name):
    # 目錄日期會因遲交而倒退，正式重播順序由檔名內的首筆序號決定。
    return int(Path(name).name.split('_', 1)[0].split('.', 1)[0])


def open_sealed(root, name):
    # 封段在開檔前可能被壓縮或搬移，重試涵蓋這兩次合法的位置變更。
    for attempt in range(3):
        try:
            path = location(root, name)
            return gzip.open(path, 'rb') if path.suffix == '.gz' else path.open('rb')
        except FileNotFoundError:
            if attempt == 2:
                raise


def archive_entries(root, cursor, last, day=None):
    # 唯讀備份可只保留 manifest 與 JSONL；以原始起始序號及停止筆數重建目錄。
    names = {name.removesuffix('.gz') for name in disk_names(root, '*.jsonl*')
             if name.endswith(('.jsonl', '.jsonl.gz'))}
    names.update(cold_names(root))
    starts = sorted((first_seq(name), name) for name in names)
    if (not starts and last) or (starts and (starts[0][0] != 1 or starts[-1][0] > last)):
        raise ValueError('archive does not match stopped manifest')
    for index, (first, name) in enumerate(starts):
        end = starts[index + 1][0] - 1 if index + 1 < len(starts) else last
        if end < first:
            raise ValueError('invalid archived sequence range')
        if end > cursor:
            if day and Path(name).parent.as_posix() not in ('.', day):
                # 無 SQLite 的備份靠鄰段推算末序號；也須查證略過的日期，以免缺段被錯歸到鄰日。
                for _ in segment_lines(root, name, first, end):
                    pass
            yield name, first, end


def catalog_entries(root, cursor, last, *, limit=None, day=None):
    # 即時目錄固定已提交上限，跨段讀資料期間釋放讀取交易，避免阻礙 WAL。
    db = None
    try:
        try:
            uri = (root / '_state/merge.sqlite').as_uri() + '?mode=ro'
            db = sqlite3.connect(uri, uri=True)
            end = db.execute('SELECT coalesce(max(last_seq),0) FROM segments').fetchone()[0]
        except sqlite3.OperationalError:
            if last is None:
                raise
            # 唯讀掛載無法建立 SQLite 的 WAL 輔助檔；停止批次可直接核對封段。
            yield from archive_entries(root, cursor, last, day)
            return
        if cursor > end or (last is not None and last != end):
            raise ValueError('sealed catalog does not match source or export cursor')
        if limit is not None:
            if not cursor <= limit <= end:
                raise ValueError('sealed range exceeds catalog')
            end = limit
        resume = cursor
        while cursor < end:
            row = db.execute('SELECT name,first_seq,last_seq FROM segments WHERE last_seq>? '
                             'AND last_seq<=? ORDER BY last_seq LIMIT 1', (cursor, end)).fetchone()
            if not row or not row[1] <= cursor + 1 <= row[2] or (cursor != resume and row[1] != cursor + 1):
                raise ValueError('sealed segment catalog gap')
            if cursor == 0 and row[1] != 1:
                raise ValueError('invalid first sealed sequence')
            yield row
            cursor = row[2]
    finally:
        if db is not None:
            db.close()


def segment_lines(root, name, first, last):
    # 保留原文位元組並逐行核對正式序號；缺行、額外行與截斷都中止匯出。
    expected = first
    with open_sealed(root, name) as source:
        for line in source:
            record = json.loads(line)
            if not line.endswith(b'\n') or record['seq'] != expected or expected > last:
                raise ValueError('sealed JSONL sequence or line mismatch')
            expected += 1
            yield record, line
        if expected != last + 1:
            raise ValueError('sealed JSONL truncated')


def segment_batches(root, name, first, last, cursor=0):
    # 控制解碼後的批量，已確認的前綴仍會接受檔案完整性檢查。
    batch = []
    size = 0
    for record, line in segment_lines(root, name, first, last):
        if record['seq'] <= cursor:
            continue
        batch.append(record)
        size += len(line)
        if size >= 8 * 1024**2 or len(batch) == 8192:
            yield batch
            batch, size = [], 0
    if batch:
        yield batch


def utc_day(value):
    # 僅接受一致的 ISO 日期字串，供命令列、輸出目錄及游標綁定共用。
    parsed = datetime.strptime(value, '%Y-%m-%d')
    if parsed.date().isoformat() != value:
        raise ValueError('expected UTC date YYYY-MM-DD')
    return value


def sealed_snapshot(root, day):
    # 在相同 SQLite 快照取得封段末端與開放段，避免換日的心跳尚未封尾就匯出。
    if day >= datetime.now(timezone.utc).date().isoformat():
        raise ValueError('live daily processing requires a past UTC date')
    uri = (Path(root).resolve() / '_state/merge.sqlite').as_uri() + '?mode=ro'
    with closing(sqlite3.connect(uri, uri=True)) as db:
        end, path, size = db.execute('SELECT (SELECT coalesce(max(last_seq),0) FROM segments),path,offset FROM output').fetchone()
    if path and size and Path(path).parent.name != 'raw' and Path(path).parent.name <= day:
        raise ValueError('requested date range still has an open segment; retry after sealing')
    return end
