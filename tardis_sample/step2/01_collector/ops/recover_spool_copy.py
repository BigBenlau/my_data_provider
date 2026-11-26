"""在故障批次的獨立副本內，使用原版映像離線提交已落盤的 worker 資料。"""
import argparse
from contextlib import closing
import fcntl
from importlib import import_module
import json
from pathlib import Path
import shutil
import sqlite3

from common import LOG, atomic_json, configure_logging

MergeStore = import_module('01_collector.delivery').MergeStore
identity = import_module('01_collector.redundancy').identity


def replay_worker(store, path):
    # 以唯讀方式開啟 spool，保留 WAL 中已提交的資料和所有原始待交付記錄。
    worker = path.parent.name
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
        written, ack = db.execute('SELECT seq,ack FROM state WHERE id=1').fetchone()
        first = cursor = store.cursor(worker)
        if not ack <= cursor <= written:
            raise ValueError(f'{worker}: inconsistent cursors ack={ack} committed={cursor} durable={written}')
        batch = []; size = 0
        for seq, payload in db.execute('SELECT seq,payload FROM items WHERE seq>? ORDER BY seq', (cursor,)):
            if seq > written:
                raise ValueError(f'{worker}: spool row {seq} exceeds durable cursor {written}')
            batch.append(dict(seq=seq, record=json.loads(payload)))
            size += len(payload.encode())
            if len(batch) >= 512 or size >= 8 * 1024**2:
                cursor = store.ingest(worker, batch)
                batch = []; size = 0
        if batch:
            cursor = store.ingest(worker, batch)
        if cursor != written:
            raise ValueError(f'{worker}: spool ended at {cursor}, expected {written}')
        LOG.info('event=spool_copy_recovered worker=%s previous_cursor=%s durable=%s', worker, first, written)
        return dict(previous_cursor=first, committed=cursor, durable=written, original_ack=ack)


def snapshot_stamp(snapshot):
    # 主檔、WAL、未封段 JSONL 與目錄時間用於識別凍結快照，共享記憶體檔可重建。
    files = [snapshot / 'manifest.json', snapshot / 'raw', *(snapshot / 'raw').glob('*.part'),
             *(snapshot / '_state').glob('*.sqlite*'), *snapshot.glob('_spool/*/spool.sqlite*')]
    stamp = {}
    for path in files:
        if not path.name.endswith('-shm'):
            stat = path.stat()
            stamp[str(path.relative_to(snapshot))] = [stat.st_size, stat.st_mtime_ns]
    return stamp


def prepare_copy(snapshot, root, image):
    # 只寫入新建的獨立目錄；重試時核對唯讀快照及先前已完成的複製標記。
    if root.is_relative_to(snapshot) or snapshot.is_relative_to(root):
        raise ValueError('snapshot and recovery directory overlap')
    manifest = json.loads((snapshot / 'manifest.json').read_text())
    config = json.loads((snapshot / '_state/config.json').read_text())
    if manifest['identity'] != identity(config, image) or manifest['config'] != config:
        raise ValueError('original code, image and configuration must match the manifest')
    if not (snapshot / '_state/merge.sqlite').is_file():
        raise ValueError('missing supervisor merge database')
    stamp = snapshot_stamp(snapshot)
    marker = root / 'spool-recovery-source.json'
    if root.exists():
        if not marker.is_file() or json.loads(marker.read_text()) != stamp:
            raise ValueError('choose a new output directory for this snapshot')
        if not (root / '_state/merge.sqlite').is_file() or not (root / 'raw').is_dir():
            raise ValueError('incomplete working copy; restart with a new output directory')
        return config
    root.mkdir(parents=True)
    shutil.copy2(snapshot / 'manifest.json', root / 'manifest.json')
    for directory in ('_state', 'raw', '_spool'):
        source = snapshot / directory
        if directory == '_spool' and not source.exists():
            continue
        shutil.copytree(source, root / directory, ignore=shutil.ignore_patterns('*.sock', '*.sqlite-shm'))
    if stamp != snapshot_stamp(snapshot):
        raise ValueError('source changed during copy; use a frozen snapshot and a new output directory')
    atomic_json(marker, stamp)
    return config


def recover_copy(snapshot, root, image):
    # 使用原版去重及提交邏輯；資料庫和維護鎖都位於獨立副本。
    root = root.resolve()
    snapshot = snapshot.resolve()
    with (snapshot / '.collector.lock').open('rb') as source_lock:
        fcntl.flock(source_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        config = prepare_copy(snapshot, root, image)
    paths = sorted((root / '_spool').glob('*/spool.sqlite'))
    roles = json.loads((root / '_state/supervisor.json').read_text())
    expected = set(roles['active'])
    expected.update(roles[r]['worker'] for r in ('candidate', 'retiring') if roles.get(r))
    missing = expected - {p.parent.name for p in paths}
    if missing:
        raise ValueError(f'missing spool for assigned worker(s): {sorted(missing)}')
    with (root / '.collector.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        store = MergeStore(root, config)
        try:
            workers = {p.parent.name: replay_worker(store, p) for p in paths}
            written = store.db.execute('SELECT written FROM output WHERE id=1').fetchone()[0]
        finally:
            store.close()
        # 封段後另存復原報告；原 manifest、spool 與事故品質紀錄保留供核對。
        report = dict(workers=workers, raw_records=written, coverage='unverified_after_storage_failure',
                      tail_order='worker_id_then_delivery_sequence')
        atomic_json(root / 'spool-recovery.json', report)
        return report


def main():
    # 由舊映像載入當時的程式，來源掛載唯讀，所有結果寫入獨立目的地。
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--image-id', required=True)
    args = parser.parse_args()
    configure_logging()
    print(json.dumps(recover_copy(args.snapshot, args.output, args.image_id), ensure_ascii=False))


if __name__ == '__main__':
    main()
