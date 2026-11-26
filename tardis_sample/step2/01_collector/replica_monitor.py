"""副本覆蓋、序號觀察及保守交接判定；不建立或修補訂單簿。"""
import json
import time
from common import LOG
from .transport import adapter, key
from .redundancy import configured_feeds, feed_groups, source_inventory


class Monitor:
    """監測使用提交證據；程序存活或訂閱 ACK 都不等於資料完整。"""
    def __init__(self, store, config):
        # 與 MergeStore 共用序列化鎖，所有查詢由 supervisor 排入同一執行區。
        self.store = store; self.db = store.db; self.config = config
        self.options = config['redundancy']; self.feeds = configured_feeds(config)
        self.groups = {g['id']: g for g in feed_groups(config)}
        self.feed_groups = {key(f):gid for gid,g in self.groups.items() for f in g['feeds']}
        self.reported = {}; self.latest = {}; self.cooldowns = {}
        self.coverage = {}; self.proofs = {}
        self.delivery_samples = {}
        self.backlogged = False; self.clear_samples = 0; self.peaks = {}; self.clear_reports = {}
        self.current = {w: json.loads(s) for w, s in self.db.execute('SELECT worker,status FROM worker_state')}
        self.blockers = {key(f): 'no_reliable_native_identity' for f in self.feeds
                         if f['channel'] not in adapter(f['exchange']).replica_strong_channels}

    def status(self, worker, value):
        # 使用 supervisor 接收時間判定 IPC 健康，避免 worker 自報未來時間。
        now = time.time()
        self.current[worker] = dict(value, updated=now, reported_at=min(now, value.get('updated', now)))

    def states(self):
        # 重啟後沿用最後狀態，但過期狀態不參與安全交接。
        return dict(self.current)

    def forget(self, worker):
        # 已排空退役的 worker 不再保留大型健康快照；歷史交付游標仍保存。
        self.current.pop(worker, None)
        self.delivery_samples.pop(worker, None)
        with self.db:
            self.db.execute('DELETE FROM worker_state WHERE worker=?', (worker,))
            self.db.execute('DELETE FROM markers WHERE worker=?', (worker,))
        self.reported = {k:v for k,v in self.reported.items() if k[0] != worker}
        self.cooldowns = {k:v for k,v in self.cooldowns.items() if k[0] != worker}

    def common(self, left, right, feed, states, since):
        # 只採目前連線 session 的共同原生強身分事件，排除先前世代的證據。
        a = states.get(left, {}).get('feeds', {}).get(feed, {})
        b = states.get(right, {}).get('feeds', {}).get(feed, {})
        if not all(s.get('connected') and s.get('subscribed') for s in (a, b)): return 0
        needed = self.options['match_events']
        paths = [(left, a['session']), (right, b['session'])]
        for n in range(needed, 0, -1):
            if self.store.evidence.common(self.store.feed_key(feed), paths, since, n):
                return n
        return 0

    def verify(self, candidate, peers, since):
        # 每個來源都必須通過；清算靜默及無可靠身分的來源不能被其他 channel 掩蓋。
        states = self.states(); now = time.time(); reasons = dict(self.blockers)
        if now - since < self.options['verify_seconds']: return False, {'observation': 'too_short'}
        for worker in (candidate, *peers):
            if now - states.get(worker, {}).get('reported_at', 0) > self.options['worker_timeout']:
                reasons[worker] = 'transport_stale'
        for feed in self.feeds:
            name = key(feed); needed = self.match_count(feed)
            if any(self.common(candidate, p, name, states, since) < needed for p in peers):
                reasons[name] = 'common_native_events_unverified'
            if self.db.execute('SELECT 1 FROM conflicts WHERE feed=? AND created>=? LIMIT 1', (name, since)).fetchone():
                reasons[name] = 'content_conflict'
            if self.db.execute('SELECT 1 FROM gaps WHERE feed=? AND created>=? AND resolved<>1 LIMIT 1', (name, since)).fetchone():
                reasons[name] = 'unresolved_gap'
        return not reasons, reasons

    def chain(self,feed,start,end):
        # 只查與缺口相交的序號邊，冷盤不可用時於恢復後再次排入。
        import heapq
        import sqlite3
        sql = 'SELECT u,previous,first,reset FROM {} WHERE feed=? AND u>? AND u<=? ORDER BY u'
        args = (feed,start,end)
        hot = self.db.execute(sql.format('all_edges'),args)
        edges = hot
        if self.store.cold.populated:
            try:
                old = self.store.cold.connect().execute(sql.format('edges'),args).fetchall()
                edges = heapq.merge(hot,old,key=lambda row:row[0])
            except (OSError,sqlite3.Error):
                self.db.execute('INSERT OR IGNORE INTO cold_gap_retry SELECT rowid FROM gaps WHERE feed=? AND start_u=? AND end_u=?',args)
        cursor = start
        for u,prev,first,reset in edges:
            if not reset and (prev==cursor or (prev is None and first is not None and first<=cursor+1)):
                cursor=max(cursor,u)
        return cursor

    def resolve_gaps(self):
        # 用跨路序號邊串接缺口；無法串接只回報未覆蓋，不憑較大 u 宣稱已補齊。
        now = time.time(); unresolved = []
        rows = self.db.execute('SELECT rowid,worker,feed,start_u,end_u,created,resolved FROM gaps WHERE rowid IN (SELECT gap FROM dirty_gaps)').fetchall()
        with self.db:
            for ident, worker, feed, start, end, created, resolved in rows:
                self.db.execute('DELETE FROM dirty_gaps WHERE gap=?', (ident,))
                cursor = self.chain(feed,start,end)
                if cursor >= end:
                    self.db.execute('UPDATE gaps SET resolved=1 WHERE rowid=?', (ident,))
                    LOG.warning('event=single_path_gap_covered worker=%s feed=%s from=%s to=%s', worker, feed, start, end)
                elif now - created >= self.options['match_wait_seconds']:
                    unresolved.append(feed)
                    if resolved == 0:
                        LOG.error('event=uncovered_sequence_gap worker=%s feed=%s from=%s to=%s', worker, feed, start, end)
                        self.db.execute('UPDATE gaps SET resolved=2 WHERE rowid=?', (ident,))
            due = now - self.options['match_wait_seconds']
            for worker, feed, start, end in self.db.execute('SELECT worker,feed,start_u,end_u FROM gaps WHERE resolved=0 AND created<=?', (due,)):
                LOG.error('event=uncovered_sequence_gap worker=%s feed=%s from=%s to=%s', worker, feed, start, end)
            self.db.execute('UPDATE gaps SET resolved=2 WHERE resolved=0 AND created<=?', (due,))
        return [r[0] for r in self.db.execute('SELECT DISTINCT feed FROM gaps WHERE resolved=2 ORDER BY feed')]

    def repairs(self, workers, states):
        # 來源靜默只回報；重連必須有整組停止收訊與持續跨路進度兩項證據。
        commands = {w: [] for w in workers}; now = time.time(); quiet = []
        self.reported = {k:v for k,v in self.reported.items() if now-v[0] < 120 and k[0] in workers}
        self.cooldowns = {k:v for k,v in self.cooldowns.items() if now-v < self.options.get('repair_cooldown_seconds',60) and k[0] in workers}
        for feed in self.feeds:
            name = key(feed)
            newest = max((states.get(w, {}).get('feeds', {}).get(name, {}).get('last_data_us') or 0 for w in workers), default=0)
            threshold = adapter(feed['exchange']).replica_silence_seconds.get(feed['channel'])
            if not newest or (threshold and now - newest / 1e6 > threshold): quiet.append(name)
        for name, group in self.groups.items():
            values = {w: self.group_state(group, states.get(w, {}), now) for w in workers}
            for worker, value in values.items():
                peers = [v for w, v in values.items() if w != worker and v]
                if not value or not peers: continue
                command = self.repair_command(worker, name, value, max(peers, key=lambda v: v['last']), now)
                if command: commands[worker].append(command)
        for worker,feed,session,reason,created in self.db.execute('SELECT * FROM session_faults WHERE created>=?',(now-60,)):
            state = states.get(worker,{}).get('feeds',{}).get(feed,{})
            if worker in workers and feed in self.feed_groups and state.get('session')==session:
                commands[worker].append(dict(group=self.feed_groups[feed],session=session,trigger=reason,created=created,expires=created+60))
        return commands, quiet

    def group_state(self, group, state, now):
        # 過期控制狀態不參與修復；新 session 必須先取得足夠觀察時間。
        if now - state.get('reported_at', 0) > self.options['match_wait_seconds']: return None
        values = [state.get('feeds', {}).get(key(f), {}) for f in group['feeds']]
        if not all(v.get('connected') and v.get('subscribed') for v in values): return None
        return dict(session=values[0]['session'], last=max(v.get('last_data_us') or 0 for v in values),
                    started=max(v.get('session_started_us', int(now * 1e6)) for v in values),
                    received=sum(v.get('received', 0) for v in values))

    def repair_command(self, worker, group, value, peer, now):
        # 冷門商品不會獨自觸發整組重連；確認持續停滯後每個 session 至多發一次。
        identity = (worker, group, value['session']); cooldown = (worker, group)
        grace = self.options.get('repair_grace_seconds', 15)
        stale = now - max(value['last'], value['started']) / 1e6 > grace
        if not stale or now - peer['last'] / 1e6 > self.options['match_wait_seconds']:
            self.reported.pop(identity, None); return None
        if now - self.cooldowns.get(cooldown, 0) < self.options.get('repair_cooldown_seconds', 60): return None
        observation = self.reported.setdefault(identity, (now, peer['session'], peer['received']))
        if observation[1] != peer['session']:
            self.reported[identity] = (now, peer['session'], peer['received']); return None
        if now - observation[0] < self.options['match_wait_seconds'] or peer['received'] - observation[2] < 3: return None
        self.cooldowns[cooldown] = now; self.reported.pop(identity, None)
        LOG.warning('event=group_receive_stalled worker=%s group=%s session=%s action=verify_candidate', worker, group, value['session'])
        return dict(group=group, session=value['session'], last_data_us=value['last'], expires=now + 5)

    def coverage_state(self, feed, peers, states, now):
        # 只比較現役兩路的新鮮狀態；候選連線不能補足舊路的覆蓋數字。
        name = key(feed)
        paths = {}
        for peer in peers:
            state = states.get(peer, {})
            value = state.get('feeds', {}).get(name, {})
            if now - state.get('reported_at', 0) <= self.options['worker_timeout']:
                if value.get('connected') and value.get('subscribed') and value.get('first_data_us'):
                    paths[peer] = (value['session'], value['first_data_us'])
        pair = tuple((p, s) for p, (s, _) in paths.items())
        old = self.coverage.get(name, {})
        report = dict(paths=len(paths), sessions=dict(pair), state=('single_path' if paths else 'waiting'))
        if paths:
            report['first_receive_us'] = min(v[1] for v in paths.values())
        if old.get('sessions') != report['sessions']:
            self.proofs.pop(name, None)
        if len(paths) == 2:
            report['both_receive_us'] = max(v[1] for v in paths.values())
            report['state'] = 'observed_only' if name in self.blockers else 'awaiting_match'
            if old.get('sessions') == report['sessions'] and old.get('matched_observed_us'):
                report.update(state='matched', matched_observed_us=old['matched_observed_us'])
            elif name not in self.blockers and self.sample_match(name, pair, self.match_count(feed)):
                report.update(state='matched', matched_observed_us=int(now * 1e6))
        if old != report:
            LOG.info('event=ws_coverage feed=%s details=%s', name, json.dumps(report))
        return report

    def match_count(self, feed):
        # 事件稀少的來源由交易所明確聲明，交接仍需實際共同事件。
        return 1 if feed['channel'] in adapter(feed['exchange']).optional_channels else self.options['match_events']

    def sample_match(self, name, pair, needed):
        # 逐一核對有限樣本的主鍵，避免初始收訊或冷門來源掃描整段歷史。
        matched = self.proofs.setdefault(name, set())
        for seq in self.store.samples.get(name, ()):
            if seq in matched:
                continue
            rows = self.db.execute('SELECT path FROM observations WHERE seq=? AND feed=? AND strong=1', (seq,self.store.feed_key(name)))
            if {self.store.evidence.path(*p) for p in pair}.issubset({r[0] for r in rows}):
                matched.add(seq)
                if len(matched) >= needed:
                    return True
        return False

    def delivery_rate(self, worker, state, now):
        # 比較同一程序的兩份新鮮回報；重啟、倒退或過期樣本都不推算速率。
        fields = ('started', 'updated', 'enqueued', 'ack', 'pending_bytes')
        if any(state.get(field) is None for field in fields):
            return {}
        previous = self.delivery_samples.get(worker)
        sample = {field: state[field] for field in fields}
        sample['written_bytes'] = state.get('written_bytes',0)
        self.delivery_samples[worker] = sample
        if not previous or sample['started'] != previous['started']:
            return {}
        elapsed = sample['updated'] - previous['updated']
        max_interval = max(2 * self.options['heartbeat_seconds'], self.options['worker_timeout'])
        if not 0 < elapsed <= max_interval or not 0 <= now - sample['updated'] <= self.options['worker_timeout']:
            return {}
        received = sample['enqueued'] - previous['enqueued']
        acked = sample['ack'] - previous['ack']
        if received < 0 or acked < 0:
            return {}
        growth = (sample['pending_bytes'] - previous['pending_bytes']) / elapsed
        remaining = max(0, self.options['spool_max_bytes'] - sample['pending_bytes'])
        incoming = max(0,(sample['written_bytes']-previous['written_bytes'])/elapsed)
        self.peaks[worker] = max(incoming,self.peaks.get(worker,0))
        peak = self.peaks[worker]
        extra = dict(peak_spool_bytes_per_s=round(peak,1),pause_capacity_seconds=round(remaining/peak,1) if peak else None) if 'written_bytes' in state else {}
        return dict(**extra,enqueued_per_s=round(received / elapsed, 1), ack_per_s=round(acked / elapsed, 1),
                    pending_bytes_per_s=round(growth, 1), seconds_to_limit=round(remaining / growth, 1) if growth > 0 else None)

    def pressure(self, workers, states):
        # 高水位立即暫停例行工作，連續三個新鮮的低水位心跳才解除。
        values = [states.get(w,{}) for w in workers]
        cap = self.options['spool_max_bytes']
        if any(v.get('oldest_pending_s',0)>10 or v.get('pending_bytes',0)>=.8*cap for v in values):
            self.backlogged = True
            self.clear_samples = 0
            self.clear_reports = {}
        elif self.backlogged:
            clear = values and all(time.time()-v.get('reported_at',0)<=self.options['worker_timeout']
                                   and v.get('oldest_pending_s',0)<5 and v.get('pending_bytes',0)<.6*cap for v in values)
            stamps = {w:states.get(w,{}).get('reported_at',0) for w in workers}
            if clear and any(t<=self.clear_reports.get(w,0) for w,t in stamps.items()):
                return
            self.clear_reports = stamps
            self.clear_samples = self.clear_samples + 1 if clear else 0
            if self.clear_samples >= 3:
                self.backlogged = False

    def health(self, workers, active=None):
        # 每五秒彙總程序外的傳輸、收訊、交付及落盤狀態，明示不可證明完整。
        states = self.states(); self.pressure(workers, states)
        repairs, quiet = self.repairs(workers, states)
        if workers:
            peers = workers[:2] if active is None else active
            now = time.time()
            self.coverage = {key(f): self.coverage_state(f, peers, states, now) for f in self.feeds}
        else:
            quiet = []
        with self.db:
            self.db.executemany('INSERT OR REPLACE INTO worker_state VALUES(?,?,?)',
                                [(w, json.dumps(s), s['updated']) for w, s in states.items()])
        gaps = self.resolve_gaps(); written = self.db.execute('SELECT written FROM output').fetchone()[0]
        uncertain = dict(self.db.execute('SELECT name,value FROM totals')).get('identity_unverified', 0)
        rates = {}
        for worker in workers:
            s = states.get(worker, {})
            rates[worker] = self.delivery_rate(worker, s, time.time())
            used = s.get('pending_bytes', 0); limit = self.options['spool_max_bytes']
            if s.get('oldest_pending_s', 0) > 10 or used >= .8 * limit:
                LOG.warning('event=delivery_backlog worker=%s oldest_pending_s=%.3f pending_bytes=%s limit_bytes=%s used_pct=%.2f trend=%s action=inspect_commit_capacity',
                            worker, s.get('oldest_pending_s', 0), used, limit, 100 * used / limit, json.dumps(rates[worker]))
            LOG.info('event=worker_health worker=%s ipc_age_s=%.1f feeds_receiving=%s enqueued=%s durable=%s ack=%s pending_bytes=%s jsonl_seq=%s oldest_pending_s=%.3f ack_ms=%.3f memory_bytes=%s',
                     worker, time.time() - s.get('updated', time.time()),
                     sum(bool(f.get('last_data_us')) for f in s.get('feeds', {}).values()),
                     s.get('enqueued'), s.get('written'), s.get('ack'), s.get('pending_bytes'), written,
                     s.get('oldest_pending_s', 0), s.get('ack_ms', 0), s.get('memory_bytes', 0))
        self.latest = dict(updated_us=time.time_ns() // 1000, workers=states, raw_records=written,
                           silent_or_unverified=quiet, uncovered_gaps=gaps, identity_unverified_events=uncertain,
                           data_quality='observed_not_proven_complete', rotation_blockers=self.blockers,
                           delivery_backlog=self.backlogged, batch_metrics=self.store.metrics, ws_coverage=self.coverage, delivery_rates=rates)
        LOG.info('event=commit_health metrics=%s', json.dumps(self.store.metrics))
        LOG.info('event=supervisor_health jsonl_seq=%s silent_sources=%s uncovered_gaps=%s ambiguity_preserved=%s', written, len(quiet), len(gaps), uncertain)
        return repairs, gaps

    def snapshot_requests(self):
        # 歷史缺口不會被新快照補回；每個缺口只要求一次重新初始化。
        return self.db.execute('SELECT rowid,feed FROM gaps WHERE resolved=2 AND rowid NOT IN (SELECT gap FROM snapshot_requests)').fetchall()

    def stopped(self, storage_blocked=()):
        # 停止收訊不再標為即時靜默；必要來源缺失仍留在最終品質報告。
        observed = self.store.observed_feeds()
        inventory = source_inventory(self.config)
        absent = sorted(set(inventory) - observed)
        optional = [k for k in absent if inventory[k]['channel'] in adapter(inventory[k]['exchange']).optional_channels]
        required = sorted(set(absent) - set(optional))
        self.latest.update(status='stopped', workers={}, silent_or_unverified=[], observed_sources=sorted(observed),
                           missing_required_sources=required, silent_optional_sources=optional)
        if required: self.latest['data_quality'] = 'incomplete'
        if storage_blocked:
            self.latest.update(status='storage_blocked', data_quality='incomplete')
            LOG.error('event=capture_stopped_with_pending_spool workers=%s raw_records=%s', storage_blocked, self.latest['raw_records'])
        else:
            LOG.info('event=capture_stopped raw_records=%s missing_required=%s silent_optional=%s',
                     self.latest['raw_records'], len(required), len(optional))
        return self.latest

    def snapshot_requested(self, gap):
        # 排程已持久化後才確認；崩潰重送只會更新同一個共用任務。
        with self.db: self.db.execute('INSERT OR IGNORE INTO snapshot_requests VALUES(?)', (gap,))
