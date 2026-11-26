"""將長批次的清單、逐檔計數與完整缺失明細保存在暫存磁碟索引。"""
import json
import os
from pathlib import Path

from common import LOG


class ReplayJournal:
    def __init__(self, db):
        # 批次計畫、警告和歷史日期的統計都不隨行情筆數堆在 Python 記憶體。
        self.db = db
        self.warning_count = 0
        self.source_issues = False
        db.executescript('''
            PRAGMA temp_store=FILE;
            CREATE TABLE runs(path TEXT UNIQUE, started INTEGER, ended INTEGER, manifest TEXT, input TEXT);
            CREATE INDEX run_order ON runs(started,path);
            CREATE TABLE warnings(payload TEXT);
            CREATE TABLE files(path TEXT PRIMARY KEY, count INTEGER, low INTEGER, high INTEGER);
        ''')

    def append(self, warning):
        # 每個缺失立即寫 log，完整明細串流寫入最終報告。
        payload = json.dumps(warning, ensure_ascii=False)
        LOG.warning('event=normalization_warning %s', payload)
        self.db.execute('INSERT INTO warnings VALUES (?)', (payload,))
        self.warning_count += 1

    def plan(self, runs):
        # 磁碟排序 manifest，一次只取一份大型 health；相同開始時間依路徑排序。
        for root, manifest in runs:
            start = manifest['started_us']
            self.db.execute('INSERT INTO runs VALUES (?,?,?,?,NULL)', (
                str(root), start, manifest.get('ended_us', start), json.dumps(manifest)))
        previous = None
        for start, end in self.db.execute('SELECT started,ended FROM runs ORDER BY started,path'):
            if previous is not None and start < previous:
                raise ValueError('overlapping runs require explicit replica merging; unsupported')
            previous = end
        self.db.commit()

    def runs(self):
        # 重播時逐份取回已固定的 manifest，包含採集中批次的封段上限。
        for path, payload in self.db.execute('SELECT path,manifest FROM runs ORDER BY started,path'):
            yield Path(path), json.loads(payload)

    def started(self, root, manifest):
        # 保存實際開始處理的輸入，即使稍後有不可恢復的輸出錯誤也能追溯。
        health = manifest.get('health_at_stop')
        self.source_issues |= (health or {}).get('data_quality') != 'healthy'
        item = dict(run=str(root), manifest_status=manifest['status'], health=health)
        if 'sealed_through_seq' in manifest:
            item['sealed_through_seq'] = manifest['sealed_through_seq']
        self.db.execute('UPDATE runs SET input=? WHERE path=?', (json.dumps(item, ensure_ascii=False), str(root)))

    def save_counts(self, pending):
        # 跨批累加同一 CSV，包括接收次序較晚但事件日較早的行情。
        self.db.executemany('''INSERT INTO files VALUES (?,?,?,?) ON CONFLICT(path) DO UPDATE SET
            count=count+excluded.count, low=min(low,excluded.low), high=max(high,excluded.high)''',
            ((path, *stats) for path, stats in pending.items()))

    def sections(self, summary=False):
        # 每個元素是單筆已編碼的 JSON；寫報告與輸出 CLI 摘要都毋須載入全部路徑。
        rows = self.db.execute('SELECT path,count FROM files ORDER BY rowid')
        yield 'rows', '{', (f'{json.dumps(path)}: {count}' for path, count in rows)
        if not summary:
            ranges = self.db.execute('SELECT path,low,high FROM files ORDER BY rowid')
            yield 'timestamp_ranges', '{', (f'{json.dumps(path)}: [{low}, {high}]' for path, low, high in ranges)
            inputs = self.db.execute('SELECT input FROM runs WHERE input IS NOT NULL ORDER BY started,path')
            yield 'inputs', '[', (row[0] for row in inputs)
            warnings = self.db.execute('SELECT payload FROM warnings ORDER BY rowid')
            yield 'recovery_warnings', '[', (row[0] for row in warnings)

    def dump(self, stream, fields, summary=False):
        # 物件內的大集合以游標逐項寫出，保留既有 JSON 結構。
        stream.write('{')
        space = ' ' if summary else '\n  '
        for index, (key, value) in enumerate(fields.items()):
            stream.write((',' if index else '') + space + json.dumps(key) + ': ')
            json.dump(value, stream, ensure_ascii=False)
        for key, opening, items in self.sections(summary):
            stream.write(',' + space + json.dumps(key) + ': ' + opening)
            for index, item in enumerate(items):
                stream.write((',' if index else '') + space + item)
            stream.write('}' if opening == '{' else ']')
        stream.write('}\n' if summary else '\n}\n')

    def publish(self, path, report):
        # 先完整寫至同目錄暫存檔，再原子發布報告。
        temporary = path.with_suffix('.json.tmp')
        with temporary.open('w', encoding='utf-8') as stream:
            self.dump(stream, report)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
