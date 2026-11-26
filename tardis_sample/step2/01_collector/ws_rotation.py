"""主管持久化一個交接意圖，副本獨立管理原線及一條候選線。"""
import asyncio
import json
import time
import uuid
from common import LOG
from .transport import key


class GroupLines:
    def __init__(self, worker, group):
        # 每條線有自己的健康狀態，正式資料都經同一個耐久 spool。
        self.worker = worker
        self.base = group
        self.active = self.candidate = None
        self.token = None
        self.order = {}
        self.wake = asyncio.Event()
        self.completed = None

    def start(self):
        # 為此連線獨立配置狀態，以免候選重連清除現役 session。
        stats = {key(f): dict(connected=False, subscribed=False, received=0, session='', last_data_us=None)
                 for f in self.base['feeds']}
        group = dict(self.base, stats=stats)
        return stats, asyncio.create_task(self.worker.stream(group))

    def status(self):
        # 控制通道按世代回報候選收訊；舊指令不能停止新的現役連線。
        return dict(token=self.token, feeds=self.candidate[0] if self.candidate else {}, completed=self.completed)

    async def close(self, line):
        # 取消網路接收並等候連線關閉，已收訊資料仍由 spool 交付。
        if line:
            line[1].cancel()
            await asyncio.gather(line[1], return_exceptions=True)

    async def apply(self):
        # 主管失聯時讓逾期候選退出；現役斷線由自己的 stream 立即重連。
        cmd = self.order
        if not cmd or cmd.get('expires', 0) <= time.time() or cmd.get('action') == 'cancel':
            await self.close(self.candidate)
            self.candidate = self.token = None
            return
        if self.completed == cmd['token']:
            return
        active = next(iter(self.active[0].values()))
        if active['session'] != cmd['old_session']:
            await self.close(self.candidate)
            self.candidate = self.token = None
            return
        if cmd['action'] == 'open' and self.token != cmd['token']:
            await self.close(self.candidate)
            self.candidate = self.start()
            self.token = cmd['token']
        if cmd['action'] == 'promote' and self.token == cmd['token'] and self.candidate:
            if not all(s['connected'] and s['subscribed'] and s['session'] == cmd['new_session']
                       for s in self.candidate[0].values()):
                return
            old, self.active = self.active, self.candidate
            self.worker.stats.update(self.active[0])
            self.completed, self.token, self.candidate = self.token, None, None
            await self.close(old)

    def command(self,order):
        # 只有控制意圖改變才喚醒群組，健康線可一直等待網路事件。
        if order != self.order:
            self.order = order
            self.wake.set()

    async def run(self):
        # 等待指令、實際收訊任務退出或候選到期；所有群組共用控制通道。
        self.active = self.start()
        self.worker.stats.update(self.active[0])
        try:
            while not self.worker.stop.is_set():
                self.wake.clear()
                await self.apply()
                lines = [line[1] for line in (self.active,self.candidate) if line]
                changed = asyncio.create_task(self.wake.wait())
                timeout = max(0,self.order['expires']-time.time()) if self.candidate else None
                try:
                    done,_ = await asyncio.wait([changed,*lines],timeout=timeout,return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                finally:
                    changed.cancel()
                    await asyncio.gather(changed,return_exceptions=True)
        finally:
            await asyncio.gather(self.close(self.active),self.close(self.candidate))


class Rotation:
    def __init__(self, supervisor):
        # 排程與最後操作保存在主管角色檔，接管後重送同一個意圖。
        self.sup = supervisor
        self.mon = supervisor.monitor
        self.options = supervisor.options
        self.s = supervisor.state.setdefault('ws_rotation', dict(history=[], attempted={}, side=0))
        with self.mon.db:
            token = (self.s.get('trial') or {}).get('token','')
            self.mon.db.execute('UPDATE handovers SET ended=? WHERE ended IS NULL AND token<>?',(time.time(),token))
        self.last_tick = 0

    def log(self, reason, **details):
        # 每次決策進入運行日誌，狀態檔另留最近一百筆供巡檢。
        item = dict(time=time.time(), reason=reason, **details)
        self.s['history'] = (self.s['history'] + [item])[-100:]
        with self.mon.db:
            self.mon.db.execute('INSERT INTO rotation_decisions(details) VALUES(?)',(json.dumps(item),))
        LOG.info('event=ws_rotation reason=%s details=%s', reason, details)
        self.sup.save()

    def command(self, worker):
        # 指令可重送，只有持有指定現役 session 的副本可以執行。
        trial = self.s.get('trial')
        return trial if trial and trial['worker'] == worker else {}

    def finish(self, reason):
        # 只撤回候選指令。失敗時現役線保持收訊，下一週期再試。
        t = self.s.pop('trial')
        with self.mon.db:
            self.mon.db.execute('UPDATE handovers SET ended=? WHERE token=?', (time.time(),t['token']))
        self.sup.state['last_rotation'] = time.time()
        if reason == 'verified_replacement':
            self.s['last_success'] = self.sup.state['last_rotation']
        self.log(reason, group=t['group'], worker=t['worker'], token=t['token'], trigger=t['trigger'])

    def verify(self, trial, status, now):
        # 逐商品驗證共同原生事件。弱來源、事故及未同步的新線不通過。
        wid, gid = trial['worker'], trial['group']
        active = self.sup.state['active']
        peers = [w for w in active if w != wid]
        if len(peers) != 1 or any(now-self.mon.current.get(w,{}).get('reported_at',0)>self.options['worker_timeout'] for w in active):
            return 'transport_stale'
        if now-trial['since'] < self.options['verify_seconds']:
            return 'observation_too_short'
        for feed in self.mon.groups[gid]['feeds']:
            name = key(feed)
            new = status.get('feeds', {}).get(name, {})
            old = self.mon.current[wid].get('feeds', {}).get(name, {})
            peer = self.mon.current[peers[0]].get('feeds', {}).get(name, {})
            if name in self.mon.blockers:
                return 'no_reliable_native_identity'
            if old.get('session') != trial['old_session']:
                return 'old_session_changed'
            if not all(s.get('connected') and s.get('subscribed') and s.get('first_data_us') for s in (new,peer)):
                return 'awaiting_data'
            if now-new.get('session_started_us',new['first_data_us'])/1e6 < self.options['verify_seconds']:
                return 'candidate_session_too_young'
            paths = [(wid,new['session']), (peers[0],peer['session'])]
            if trial['trigger'] != 'receive_stalled':
                paths.append((wid,old['session']))
            if self.mon.db.execute('SELECT 1 FROM gaps WHERE feed=? AND resolved<>1 AND created>=? UNION ALL '
                                   'SELECT 1 FROM conflicts WHERE feed=? AND created>=? LIMIT 1',
                                   (name,trial['since'],name,trial['since'])).fetchone():
                return 'gap_or_conflict'
            if not self.mon.store.evidence.common(self.mon.store.feed_key(name), paths, trial['since'], self.mon.match_count(feed)):
                return 'common_events_unverified'
        return ''

    def progress(self, now, busy):
        # 候選健康經持久行情驗證才允許停止舊線，交接完成以副本回報為準。
        t = self.s['trial']
        state = self.mon.current.get(t['worker'], {})
        reply = state.get('lines', {}).get(t['group'], {})
        if reply.get('completed') == t['token']:
            return self.finish('verified_replacement')
        if busy and t['trigger'] == 'preventive' and t['action'] == 'open':
            return self.finish('delivery_backlog')
        if now >= t['expires'] or t['worker'] not in self.sup.state['active']:
            return self.finish(t.get('waiting', 'candidate_timeout'))
        if reply.get('token') != t['token'] or t['action'] != 'open':
            return
        reason = self.verify(t, reply, now)
        t['waiting'] = reason or 'awaiting_promotion'
        if not reason:
            earliest = self.s.get('last_success',0)+self.options['rotation_seconds']
            if now < earliest:
                if not t.get('validated'):
                    t.update(validated=True, expires=max(t['expires'],earliest+self.options['verify_seconds']+5))
                    self.log('waiting_for_rotation_slot',group=t['group'],worker=t['worker'])
                return
            t.update(action='promote', new_session=next(iter(reply['feeds'].values()))['session'])
            self.log('promotion_authorized', group=t['group'], worker=t['worker'])

    def choices(self, active, now, due):
        # 正常交替兩路，最久未嘗試的群組優先；異常與超齡排在例行之前。
        choices = []
        for idx, worker in enumerate(active):
            states = self.mon.current.get(worker, {})
            repairs = {r['group']: r for r in self.sup.repair.get(worker, [])}
            for gid, group in self.mon.groups.items():
                feeds = [states.get('feeds', {}).get(key(f), {}) for f in group['feeds']]
                session = feeds[0].get('session')
                if not session or not all(s.get('connected') for s in feeds):
                    continue
                last = self.s['attempted'].get(worker+'/'+gid, 0)
                age = now-feeds[0].get('session_started_us',now*1e6)/1e6
                repair = repairs.get(gid,{})
                last_data = max(s.get('last_data_us') or 0 for s in feeds)
                fault = (repair.get('session') == session and now <= repair.get('expires',now)
                         and last < repair.get('created',now)
                         and last_data <= repair.get('last_data_us',last_data))
                aged = age >= self.sup.config.get('ws_max_age',82800)
                if not (fault or due) or (not fault and now-last < self.options['rotation_seconds']):
                    continue
                trigger = repair.get('trigger','receive_stalled') if fault else 'age_limit' if aged else 'preventive'
                weak = any(key(f) in self.mon.blockers for f in group['feeds'])
                rank = (not fault, not aged, weak and not aged, idx != self.s['side'], last, -age, gid)
                choices.append((rank,worker,gid,session,trigger,weak))
        return sorted(choices)

    def tick(self, now, busy):
        # 同一交易所只允許一個交接，排程延期後不追趕過去的週期。
        if now-self.last_tick < 1:
            return
        self.last_tick = now
        if self.s.get('trial'):
            return self.progress(now,busy)
        active = self.sup.state['active']
        due = now-self.sup.state['last_rotation'] >= self.options['rotation_seconds']
        if not due and not any(self.sup.repair.values()):
            return
        if len(active) != 2:
            return
        choices = self.choices(active,now,due)
        if not choices:
            if due:
                self.sup.state['last_rotation'] = now
                self.log('no_connected_group')
            return
        _, worker, gid, session, trigger, weak = choices[0]
        self.sup.state['last_rotation'] = now
        self.s['attempted'][worker+'/'+gid] = now
        self.s['side'] = 1-active.index(worker)
        if busy and trigger == 'preventive':
            return self.log('delivery_backlog', group=gid, worker=worker)
        if weak and trigger == 'preventive':
            return self.log('no_reliable_native_identity', group=gid, worker=worker)
        t = dict(action='open', worker=worker, group=gid, old_session=session, token=uuid.uuid4().hex,
                 since=now, expires=now+self.options['candidate_timeout'], trigger=trigger)
        with self.mon.db:
            for feed in self.mon.groups[gid]['feeds']:
                self.mon.db.execute('INSERT INTO handovers VALUES(?,?,NULL,?)', (self.mon.store.feed_key(key(feed)),now,t['token']))
        self.s['trial'] = t
        self.log('candidate_open', group=gid,worker=worker,trigger=trigger,token=t['token'])
