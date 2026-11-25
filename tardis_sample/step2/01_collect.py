#!/usr/bin/env python3
"""第 01 階段：原始採集、副本整合與原始 channel 重播；不執行 Tardis 正規化。"""
import argparse
import asyncio
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import threading
import uuid
import aiohttp
from common import ADAPTERS, adapter, configuration, feeds, atomic_json, reason, utc, configure_logging, close_log
from raw import Writer, StorageError, SCHEMA_VERSION as CHANNEL_CSV_SCHEMA_VERSION, export_channels

LOG=logging.getLogger('market-data')

def prepare_run(args, config):
    # 鎖定輸出目錄，建立獨立批次與來源版本；初始化失敗也必須釋放鎖。
    base = Path(args.output).resolve()
    base.mkdir(parents=True, exist_ok=True)
    lock = (base / '.collector.lock').open('a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run_id = utc(time.time_ns() // 1000, '%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex[:8]
        root = base / run_id
        root.mkdir()
        code_root = Path(__file__).parent
        code = [*code_root.glob('*.py'), *code_root.joinpath('collectors').glob('*.py')]
        manifest = {
            'schema_version': 2, 'run_id': run_id, 'started_us': time.time_ns() // 1000,
            'status': 'running', 'config': config,
            'channel_csv_schema_version': CHANNEL_CSV_SCHEMA_VERSION,
            'outputs': ['raw_jsonl', 'raw_channel_csv'],
            'processing': 'raw_capture', 'order_book_reconstruction': False,
            'code_sha256': {str(p.relative_to(code_root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in code},
        }
        atomic_json(root / 'manifest.json', manifest)
        atomic_json(base / 'latest.json', {'run': str(root)})
        writer = Writer(root, config['queue_capacity'], config['segment_seconds'], config['min_free_gib'],
                        channel_config=config, run_id=run_id, started_us=manifest['started_us'])
        return root, manifest, writer, lock
    except BaseException:
        lock.close()
        raise


def source_tasks(collector, config, stop, duration):
    # 每條來源獨立啟動；任務全部由呼叫端統一取消，避免留下背景採集。
    tasks = []
    for index, feed in enumerate(feeds(config)):
        collector.stat(feed)
        tasks.append(asyncio.create_task(collector.stream(feed, index * collector.connection_spacing)))
    if 'binance-futures' in config['markets']:
        for symbol in config['symbols']:
            feed = dict(exchange='binance-futures', symbol=symbol, channel='openInterest', time_unit='ms')
            collector.stat(feed)
            tasks.append(asyncio.create_task(collector.oi(feed)))
    tasks.extend([asyncio.create_task(collector.heartbeat()), asyncio.create_task(stop.wait())])
    if duration:
        tasks.append(asyncio.create_task(asyncio.sleep(duration)))
    return tasks


async def collect_until_stop(collector, args, stop):
    # 等待時限、訊號或任何任務失敗；先清理網路任務，之後才能關閉 HTTP session。
    tasks = source_tasks(collector, collector.config, stop, args.duration)
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
        LOG.info('event=stop_requested reason=%s', 'signal' if stop.is_set() else 'duration_reached')
        return collector.health()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def finish_run(root, manifest, writer, health, failure, code):
    # 排空寫入佇列後才更新結束狀態；磁碟故障使用專屬退出碼阻止無效重啟。
    try:
        await asyncio.to_thread(writer.close)
    except StorageError as exc:
        failure, code = reason(exc), 74
    manifest.update(ended_us=time.time_ns() // 1000, status='failed' if failure else 'stopped',
                    error=failure, raw_records=writer.seq, health_at_stop=health,
                    channel_csv_rows=writer.channel_rows, channel_csv_counts=writer.channel_counts,
                    channel_csv_data_counts=writer.channel_data_counts)
    try:
        atomic_json(root / 'manifest.json', manifest)
    except OSError:
        code = 74
        LOG.exception('event=manifest_write_failed path=%s', root / 'manifest.json')
    LOG.info('event=collector_stopped status=%s quality=%s raw_written=%s raw_enqueued=%s csv_rows=%s error=%s',
             manifest['status'], (health or {}).get('data_quality'), writer.written, writer.seq, writer.channel_rows, failure)
    return code


async def run(args, config):
    # 統籌單次採集生命週期；來源處理、任務清理與寫入收尾各有獨立函式。
    root, manifest, writer, lock = prepare_run(args, config)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    failure = collector = end_health = file_log = None
    code = 0
    try:
        file_log = configure_logging(root / 'collector.log', config['log_max_bytes'], config['log_backup_count'])
        # 記錄實際載入位置，方便辨識 pip 與採集程序是否使用不同環境。
        LOG.info('event=runtime python=%s executable=%s aiohttp=%s aiohttp_path=%s ws_timeout_api=%s',
                 sys.version.split()[0], sys.executable, aiohttp.__version__, aiohttp.__file__,
                 'ClientWSTimeout' if hasattr(aiohttp, 'ClientWSTimeout') else 'legacy_float')
        LOG.info('event=collector_started run=%s markets=%s symbols=%s duration_s=%s heartbeat_s=%s',
                 root, config['markets'], config['symbols'], args.duration, config['heartbeat_seconds'])
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=60)
        async with aiohttp.ClientSession(connector=connector, trust_env=False,
                timeout=aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10)) as http:
            collector = adapter(config['markets'][0])(config, root, writer, http)
            end_health = await collect_until_stop(collector, args, stop)
    except BaseException as exc:
        failure = reason(exc)
        code = 74 if isinstance(exc, StorageError) else 70
        LOG.exception('event=collector_failed')
    finally:
        if collector:
            end_health = end_health or collector.health()
        try:
            code = await finish_run(root, manifest, writer, end_health, failure, code)
        finally:
            lock.close()
            close_log(file_log)
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)
    print(json.dumps({'run': str(root), 'status': manifest['status'],
                      'data_quality': (end_health or {}).get('data_quality'), 'error': manifest['error']}))
    if not code and args.duration and end_health and end_health['unhealthy']:
        code = 2
    return code

def terminate(child):
    # 先對整個子程序群組發送 SIGTERM；逾時後用 SIGKILL，避免留下孤兒採集器。
    if child.poll() is not None:return
    os.killpg(child.pid,signal.SIGTERM)
    try:child.wait(timeout=20)
    except subprocess.TimeoutExpired:
        os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)

def child_command(args, root):
    # 子程序永遠呼叫目前的 01 入口，並完整沿用使用者選擇的設定與市場。
    cmd = [sys.executable, '-u', str(Path(__file__).resolve()), 'run', '--output', str(root)]
    if args.config:
        cmd += ['--config', str(Path(args.config).resolve())]
    if args.markets:
        cmd += ['--markets', *args.markets]
    if args.symbols:
        cmd += ['--symbols', *args.symbols]
    return cmd


def watch_child(child, root, started, timeout, stop):
    # 只採信目前子程序的心跳；舊批次的狀態不能掩蓋新程序卡住。
    while child.poll() is None and not stop.is_set():
        stop.wait(.5)
        stale = time.monotonic() - started
        try:
            run_root = Path(json.loads((root / 'latest.json').read_text())['run'])
            status = json.loads((run_root / 'status.json').read_text())
            if status.get('pid') == child.pid:
                stale = (time.time_ns() // 1000 - status['updated_us']) / 1e6
        except (OSError, ValueError, KeyError):
            pass
        if stale > timeout:
            LOG.error('event=heartbeat_stale age_s=%.1f pid=%s', stale, child.pid)
            terminate(child)
            return True
    return False


def restart_loop(args, root, stop):
    # 崩潰與心跳失效採指數退避；儲存故障停止，避免在壞磁碟上反覆啟動。
    restarts, delay = 0, 1
    while not stop.is_set():
        started = time.monotonic()
        child = subprocess.Popen(child_command(args, root), start_new_session=True)
        LOG.info('event=child_started pid=%s output=%s', child.pid, root)
        try:
            watchdog_restart = watch_child(child, root, started, args.heartbeat_timeout, stop)
        finally:
            terminate(child)
        if stop.is_set():
            return 0
        code = child.wait()
        # 看門狗終止的程序即使回傳零，也要重新啟動。
        if code == 0 and not watchdog_restart:
            return 0
        if code == 74:
            LOG.error('event=storage_failure action=stop')
            return code
        restarts += 1
        if args.restart_limit and restarts > args.restart_limit:
            return code or 1
        if time.monotonic() - started > 300:
            delay = 1
        LOG.warning('event=child_restart exit=%s delay_s=%s restart=%s', code, delay, restarts)
        stop.wait(delay)
        delay = min(60, delay * 2)
    return 0


def supervise(args):
    # 鎖定監督程序與輪替日誌，讓同一輸出根目錄只有一個重啟管理者。
    root = Path(args.output).resolve()
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / '.supervisor.lock').open('a')
    file_log = None
    previous_handlers = {}
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        config = configuration(args.config, args.markets, args.symbols)
        file_log = configure_logging(root / 'supervisor.log', config['log_max_bytes'], config['log_backup_count'])
        LOG.info('event=supervisor_started output=%s heartbeat_timeout_s=%s', root, args.heartbeat_timeout)
        stop = threading.Event()
        def stopping(*_):
            # 訊號處理只喚醒等待，由監督主循環統一結束子程序。
            stop.set()
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[sig] = signal.signal(sig, stopping)
        return restart_loop(args, root, stop)
    finally:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        LOG.info('event=supervisor_stopped output=%s', root)
        close_log(file_log)
        lock.close()


def main():
    # 解析 run／supervise／replay 命令；dry-run 只檢查設定，不建立採集任務。
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    managed_commands(commands)
    for name in ('run','supervise'):
        p=commands.add_parser(name)
        p.add_argument('--config');p.add_argument('--output',default=str(Path(__file__).parent/'output/01_raw'))
        p.add_argument('--markets',nargs='+',choices=sorted(ADAPTERS));p.add_argument('--symbols',nargs='+')
        if name=='run':
            p.add_argument('--duration',type=float);p.add_argument('--dry-run',action='store_true')
        else:
            p.add_argument('--heartbeat-timeout',type=float,default=90)
            p.add_argument('--restart-limit',type=int,default=0)
    p=commands.add_parser('replay',help='Rebuild original-channel CSV; no Tardis mapping')
    p.add_argument('--input',required=True);p.add_argument('--output',required=True);p.add_argument('--recover',action='store_true')
    args=parser.parse_args()
    configure_logging()
    if args.command in ('cluster', 'worker'):
        return managed_main(args, parser)
    if args.command == 'export-csv':
        from channel_export import Exporter
        print(json.dumps(Exporter(args.input, args.output).run()))
        return 0
    if args.command=='replay':
        result=export_channels(args.input,args.output,args.recover)
        print(json.dumps({'status':result['status'],'runs':len(result['runs'])}));return 0
    config=configuration(args.config,args.markets,args.symbols)
    if args.command=='supervise':
        if args.heartbeat_timeout<=0 or args.restart_limit<0:parser.error('invalid supervisor limits')
        return supervise(args)
    if args.duration is not None and args.duration<=0:parser.error('duration must be positive')
    if args.dry_run:
        print(json.dumps({'config':config,'feeds':feeds(config)},indent=2));return 0
    return asyncio.run(run(args,config))

def managed_commands(commands):
    # 新架構沿用 01 入口；舊 run／supervise 保留供既有操作相容。
    from redundancy import FAMILIES
    p = commands.add_parser('export-csv', help='Incrementally export sealed JSONL as channel CSV.gz')
    p.add_argument('--input', required=True, help='Supervisor run directory')
    p.add_argument('--output', required=True, help='Dedicated resumable CSV directory')
    p = commands.add_parser('cluster', help='Supervisor with two identical workers')
    p.add_argument('--exchange', choices=sorted(FAMILIES), required=True)
    p.add_argument('--config'); p.add_argument('--output', required=True)
    p.add_argument('--backend', choices=['docker', 'local'], default='docker')
    p.add_argument('--image', default='market-collector:local')
    storage = p.add_mutually_exclusive_group()
    storage.add_argument('--host-output', help='Docker host absolute path corresponding to --output')
    storage.add_argument('--data-volume', help='Docker named volume mounted at --output in the supervisor')
    p.add_argument('--duration', type=float); p.add_argument('--dry-run', action='store_true')
    p = commands.add_parser('worker', help=argparse.SUPPRESS)
    p.add_argument('--root', required=True); p.add_argument('--worker-id', required=True)
    p.add_argument('--image', required=True)


def managed_main(args, parser):
    # dry-run 不啟動 Docker、不連交易所，worker 僅接受 supervisor 封存設定。
    if args.command == 'worker':
        from worker import run_worker
        try: asyncio.run(run_worker(args.root, args.worker_id, args.image))
        except StorageError:
            LOG.exception('event=worker_storage_failed'); return 74
        return 0
    from redundancy import group_config, feed_groups, source_inventory, preflight
    if args.data_volume:
        import re
        if args.backend != 'docker' or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]+', args.data_volume):
            parser.error('data-volume requires Docker and a valid named volume')
    if args.duration is not None and args.duration <= 0: parser.error('duration must be positive')
    if args.dry_run:
        config = group_config(args.exchange, args.config)
        print(json.dumps(dict(config=config, groups=feed_groups(config), sources=source_inventory(config), preflight=preflight(config)), indent=2))
        return 0
    from supervisor import run_supervisor
    return asyncio.run(run_supervisor(args))


if __name__=='__main__':raise SystemExit(main())
