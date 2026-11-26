"""原始資料層：channel CSV、壓縮 JSONL、持久化佇列與可重複重播。"""
import base64
from collections import Counter, OrderedDict
import csv
from decimal import Decimal
import json
import os
from pathlib import Path
from common import LOG, atomic_json, utc
from common.protocol import ADAPTERS, classify
from common.raw_reader import check_count, read_records, read_runs
from .transport import adapter, decode_frame
import gzip
import queue
import shutil
import threading
import time
import fcntl

SCHEMA_VERSION = 1
# 保留既有程式可讀的原生表頭清單，實際欄位由交易所定義。
CHANNELS = {m: tuple(adapter(m).channels(m)) for m in ADAPTERS if adapter(m).native_fields}
FIELDS = {k: v for m in CHANNELS for k, v in adapter(m).native_fields.items()}
META = ('seq', 'run_id', 'kind', 'type', 'exchange', 'symbol', 'channel', 'session',
        'local_timestamp', 'time_unit', 'endpoint', 'generated', 'generation', 'encoding',
        'status', 'request_started_us', 'headers', 'extra', 'error')
ENVELOPE_FIELDS = set(META) - {'run_id', 'type', 'extra', 'error'}


def json_text(value):
    # 序列化 JSON 內的 Decimal 而不轉 float；完整字面原文另存 raw_message。
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, list):
        return '[' + ','.join(json_text(v) for v in value) + ']'
    if isinstance(value, dict):
        return '{' + ','.join(json.dumps(k, ensure_ascii=False) + ':' + json_text(v)
                              for k, v in value.items()) + '}'
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def cell(value):
    # 把單一值寫入 CSV，物件／陣列／布林保留 JSON 表示，缺失值留空。
    if value is None:
        return ''
    if isinstance(value, (dict, list, bool)):
        return json_text(value)
    return str(value)


def channel_names(exchange):
    # 從交易所 adapter 取得原生 channel 清單，讓採集與輸出一致。
    return adapter(exchange).channels(exchange)

def native_fields(exchange, channel):
    # 具有固定表頭的市場逐欄匯出，其餘保留完整原生物件。
    fields = adapter(exchange).native_fields
    return fields[channel] if fields else ('payload',)

def header(exchange, channel):
    # 組合採集 metadata、原生欄位及完整訊息原文，固定每個 channel 的欄位順序。
    return ['capture_' + k for k in META] + list(native_fields(exchange, channel)) + ['raw_message']


def channel_row(record, run_id):
    # 將一個收到的回應轉為一列；保留控制、錯誤、重複及無法解析的原文。
    if record['kind'] not in ('ws', 'rest', 'rest_error', 'message', 'data', 'probe') or not record.get('raw'):
        return None
    channel = record['channel']
    if record['exchange'] not in ADAPTERS or channel not in channel_names(record['exchange']):
        raise ValueError(f'unknown raw channel: {record["exchange"]}/{channel}')
    raw = record['raw']
    kind = record['kind']
    if kind in ('message', 'data', 'probe'):
        policy = adapter(record['exchange']).rest_policy
        kind = 'rest' if policy and channel in policy.channels else 'ws'
    data = {}
    message_type = 'rest_error' if record['kind'] == 'rest_error' else 'unparseable'
    error = ''
    try:
        decoded = base64.b64decode(raw, validate=True) if record.get('encoding') == 'base64' else raw
        obj = decode_frame(decoded, parse_float=Decimal)
        if kind == 'ws':
            message_type, payload = classify(record, obj)
            if message_type in ('ack', 'error'): message_type = 'control'
            if message_type == 'data':
                data = payload if record['exchange'] in CHANNELS else {'payload': payload}
        elif kind == 'rest':
            if isinstance(obj, dict):
                data = obj; message_type = 'data'
            else:
                message_type = 'unexpected'
    except (ValueError, TypeError, UnicodeError):
        error = 'invalid_json_or_encoding'
    row = {'capture_' + k: cell(record.get(k)) for k in ENVELOPE_FIELDS}
    row.update(capture_run_id=run_id, capture_type=message_type, capture_error=error,
               capture_extra=json_text({k: v for k, v in record.items()
                                        if k not in ENVELOPE_FIELDS and k != 'raw'}),
               raw_message=raw)
    row.update({k: cell(data.get(k)) for k in native_fields(record['exchange'], channel)})
    return row


class ChannelCSV:
    """現場採集與離線重播共用的原始 CSV 序列化器，限制同時開啟的檔案數。"""
    def __init__(self, root, run_id, config=None, started_us=None, max_open=64):
        # 初始化輸出根目錄、run ID、計數與檔案快取，按設定預建每日 header。
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.config = config or {}
        self.max_open = max_open
        self.open = OrderedDict()
        self.counts = Counter()
        self.data_counts = Counter()
        self.message_types = Counter()
        self.days = set()
        self.total = 0
        if started_us is not None:
            self.ensure_day(utc(started_us))

    @staticmethod
    def relative(exchange, symbol, day, channel):
        # 來源名稱不得含路徑跳脫，避免資料寫到輸出目錄之外。
        # 檢查市場、channel 與商品代碼，生成不會逃出輸出目錄的相對路徑。
        if exchange not in ADAPTERS or channel not in channel_names(exchange):
            raise ValueError('unknown exchange/channel')
        if not symbol or not symbol.isascii() or not symbol.replace('-', '').isalnum():
            raise ValueError('invalid symbol in raw record')
        return f'{exchange}/{symbol}/{day}/{channel}.csv'

    def ensure(self, rel, channel):
        # 以排他建立方式寫入一次 header；已有的同批檔案不重複初始化。
        if rel in self.counts:
            return
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x', encoding='utf8', newline='') as f:
            csv.writer(f, lineterminator='\n').writerow(header(rel.split('/')[0], channel))
        self.counts[rel] = 0
        self.data_counts[rel] = 0

    def ensure_day(self, day):
        # 為接收 UTC 日期建立所有設定來源的檔案，讓零事件來源仍可辨識。
        if day in self.days:
            return
        for exchange in self.config.get('markets', ()):
            settings = self.config.get('market_settings', {}).get(exchange, {})
            for symbol in settings.get('symbols', self.config.get('symbols', ())):
                for channel in settings.get('channels', channel_names(exchange)):
                    self.ensure(self.relative(exchange, symbol, day, channel), channel)
        self.days.add(day)

    def consume(self, record):
        # 按接收日期與 channel 寫入原始列；檔案快取滿時先刷出最久未使用檔案。
        day = utc(record['local_timestamp'])
        self.ensure_day(day)
        row = channel_row(record, self.run_id)
        if row is None:
            return
        rel = self.relative(record['exchange'], record['symbol'], day, record['channel'])
        self.ensure(rel, record['channel'])
        if rel not in self.open:
            if len(self.open) >= self.max_open:
                self.close_handle(self.open.popitem(last=False)[1])
            f = (self.root / rel).open('a', encoding='utf8', newline='')
            self.open[rel] = [f, csv.DictWriter(f, fieldnames=header(record['exchange'], record['channel']), lineterminator='\n'), False]
        handle = self.open[rel]
        handle[1].writerow(row); handle[2] = True
        self.open.move_to_end(rel)
        self.counts[rel] += 1; self.total += 1
        self.data_counts[rel] += row['capture_type'] == 'data'
        self.message_types[row['capture_type']] += 1

    @staticmethod
    def flush_handle(handle):
        # 只刷出有新增內容的 handle，包含 fsync，以降低空閒時的磁碟操作。
        if handle[2]:
            handle[0].flush(); os.fsync(handle[0].fileno()); handle[2] = False

    @classmethod
    def close_handle(cls, handle):
        # 先持久化再關閉檔案；即使刷出失敗也釋放 handle。
        try:
            cls.flush_handle(handle)
        finally:
            handle[0].close()

    def flush(self):
        # 持久化所有目前開啟且有新增內容的 channel CSV。
        for handle in self.open.values():
            self.flush_handle(handle)

    def close(self):
        # 關閉所有 handle 並回報寫入錯誤，不因一個失敗而遺漏其他檔案清理。
        error = None
        while self.open:
            try:
                self.close_handle(self.open.popitem()[1])
            except OSError as exc:
                error = exc
        if error:
            raise error



class StorageError(RuntimeError): 
    """無法可靠落盤時的致命錯誤；監督程序不應在壞磁碟上無限重啟。"""
    pass

class Writer:
    """單一背景執行緒按全局 seq 寫 raw 和 channel CSV；佇列有上限，禁止靜默丟資料。"""
    def __init__(self,root,capacity=100000,segment_seconds=60,min_free_gib=2,*,channel_config=None,run_id=None,started_us=None):
        # 初始化有界佇列、切檔／磁碟保留設定，啟動唯一寫入執行緒。
        self.root=Path(root); (self.root/'raw').mkdir(parents=True,exist_ok=True)
        self.queue=queue.Queue(capacity); self.segment_seconds=segment_seconds; self.min_free=min_free_gib*1024**3
        self.error=None; self.seq=0; self.written=0; self.high_water=0; self.last_write=time.monotonic()
        self.channel_config=channel_config;self.run_id=run_id or self.root.name;self.started_us=started_us
        self.channel_rows=0;self.channel_counts={};self.channel_data_counts={}
        self.thread=threading.Thread(target=self.run,name='raw-writer',daemon=True); self.thread.start()
    def emit(self,kind,exchange='',symbol='',channel='',session='',local_timestamp=None,**extra):
        # 在接收端分配 seq 並立即入列；writer 已失敗或佇列滿則立刻報錯。
        if self.error: raise StorageError(str(self.error))
        self.seq+=1
        record={'seq':self.seq,'kind':kind,'exchange':exchange,'symbol':symbol,'channel':channel,'session':session,
                'local_timestamp':local_timestamp if local_timestamp is not None else time.time_ns()//1000,**extra}
        try:self.queue.put_nowait(record)
        except queue.Full as e:raise StorageError('raw queue full; stopping rather than silently dropping') from e
        self.high_water=max(self.high_water,self.queue.qsize()); return record
    def flush_batch(self):
        # 先持久化原始 JSONL，再更新可重建的 CSV；成功後才推進已落盤序號。
        if not self.compressed:
            return
        if self.batch:
            data = ''.join(json.dumps(r, ensure_ascii=False, separators=(',', ':')) + '\n' for r in self.batch)
            self.compressed.write(data.encode())
        self.compressed.flush()
        self.handle.flush()
        os.fsync(self.handle.fileno())
        if self.channels:
            for record in self.batch:
                self.channels.consume(record)
            self.channels.flush()
            self.channel_rows = self.channels.total
        if self.batch:
            self.written = self.batch[-1]['seq']
        self.batch.clear()
        self.last_write = self.last_flush = time.monotonic()

    def close_segment(self):
        # 壓縮尾部與底層檔案均成功落盤，才把暫存段改名為完成段。
        if not self.compressed:
            return
        self.compressed.close()
        self.handle.flush()
        os.fsync(self.handle.fileno())
        self.handle.close()
        os.replace(self.path, self.path.with_suffix(''))
        self.handle = self.compressed = None
        LOG.info('event=raw_segment_closed path=%s last_seq=%s', self.path.with_suffix(''), self.written)

    def append_record(self, record, now):
        # 依持續時間或接收日期切段；檔名先放 seq，避免系統時鐘倒退改變重播順序。
        day = utc(record['local_timestamp'])
        if self.compressed and (now - self.opened >= self.segment_seconds or day != self.day):
            self.flush_batch()
            self.close_segment()
        if not self.compressed:
            self.day, self.opened = day, now
            name = f'{record["seq"]:012d}_{utc(record["local_timestamp"], "%Y-%m-%dT%H%M%S")}.jsonl.gz.part'
            self.path = self.root / 'raw' / name
            self.handle = self.path.open('xb')
            self.compressed = gzip.GzipFile(filename='', mode='wb', fileobj=self.handle, compresslevel=1, mtime=0)
            LOG.info('event=raw_segment_opened path=%s first_seq=%s', self.path, record['seq'])
        self.batch.append(record)

    def write_loop(self):
        # 限定批次大小與刷出間隔；空閒時仍檢查磁碟，停止標記必須排在已入列資料後面。
        last_disk = 0
        while True:
            try:
                record = self.queue.get(timeout=.5)
            except queue.Empty:
                record = 'flush'
            now = time.monotonic()
            if now - last_disk >= 5:
                if shutil.disk_usage(self.root).free < self.min_free:
                    raise StorageError('free disk below configured reserve')
                last_disk = now
            if record is None:
                self.flush_batch()
                self.close_segment()
                return
            if record != 'flush':
                self.append_record(record, now)
            if self.compressed and (len(self.batch) >= 512 or now - self.last_flush >= 1):
                self.flush_batch()

    def run(self):
        # 寫入狀態只由此執行緒使用；任何寫入失敗都保留暫存段並通知主程序。
        self.handle = self.compressed = self.channels = self.path = None
        self.opened = self.last_flush = 0
        self.batch = []
        try:
            if self.channel_config is not None:
                self.channels = ChannelCSV(self.root / 'channels', self.run_id, self.channel_config, self.started_us)
            self.write_loop()
        except BaseException as exc:
            self.error = exc
            LOG.exception('event=raw_writer_failed root=%s written=%s enqueued=%s', self.root, self.written, self.seq)
            # 失敗時不改名，讀取者必須明確選擇恢復未完成的資料段。
            try:
                if self.compressed:
                    self.compressed.close()
                if self.handle:
                    self.handle.close()
            except OSError:
                pass
        finally:
            if self.channels:
                self.channel_counts = dict(self.channels.counts)
                self.channel_data_counts = dict(self.channels.data_counts)
                try:
                    self.channels.close()
                except OSError as exc:
                    self.error = self.error or exc
    def close(self):
        # 送入停止標記並等待佇列排空；逾時或先前寫入失敗都必須傳回呼叫端。
        while self.thread.is_alive() and not self.error:
            try:self.queue.put(None,timeout=.2);break
            except queue.Full:pass
        self.thread.join(timeout=20)
        if self.thread.is_alive():raise StorageError('writer shutdown timeout')
        if self.error:raise StorageError(str(self.error))


def export_channels(source, output, recover=False):
    # 逐 run 重播原文成 channel CSV；不合併來源、不去重、拒絕覆寫舊輸出。
    source, output = Path(source), Path(output)
    warnings=[]; runs=list(read_runs(source,recover,warnings))
    output.mkdir(parents=True, exist_ok=False)
    report = {'status': 'running', 'channel_csv_schema_version': SCHEMA_VERSION, 'runs': [], 'recovery_warnings': warnings}
    atomic_json(output / 'channel_export_report.json', report)
    LOG.info('event=raw_replay_started input=%s output=%s runs=%s',source,output,len(runs))
    try:
        for root,manifest in runs:
            if manifest.get('channel_csv_schema_version', SCHEMA_VERSION) != SCHEMA_VERSION:
                raise ValueError('channel CSV schema changed; replay with the original exporter version')
            writer = ChannelCSV(output / root.name / 'channels', manifest.get('run_id', root.name),
                                manifest.get('config', {'markets':[manifest['exchange']], 'symbols':[manifest['symbol']]} if 'exchange' in manifest else {}), manifest['started_us'])
            count = 0
            try:
                for record in read_records(root, recover, report['recovery_warnings']):
                    writer.consume(record); count += 1
                check_count(root,manifest,count,recover,warnings)
                LOG.info('event=raw_replay_run_complete run=%s raw_records=%s csv_rows=%s',root,count,writer.total)
            finally:
                writer.close()
                report['runs'].append({'run': str(root), 'raw_records': count, 'csv_rows': writer.total,
                                       'rows': dict(writer.counts), 'data_rows': dict(writer.data_counts),
                                       'message_types': dict(writer.message_types),
                                       'source_health': manifest.get('health_at_stop')})
        report['status'] = 'completed_with_warnings' if report['recovery_warnings'] else 'complete'
    except BaseException as exc:
        report.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        report['note'] = 'Export completion is not evidence of complete market coverage. raw_message preserves the received message; no channel merging or filtering.'
        atomic_json(output / 'channel_export_report.json', report)
    return report

