"""驗證主管交接、單一寫入權與故障副本的隔離行為。"""
import asyncio
import importlib
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
import test_redundancy as fixtures

roles = importlib.import_module('01_collector.leadership')
manager = importlib.import_module('01_collector.supervisor')


class UpgradeTests(unittest.IsolatedAsyncioTestCase):
    """使用真正的 worker、SQLite 與 Unix socket 驗證新舊主管並存。"""

    settings = fixtures.RuntimeTests.settings
    fixture = fixtures.RuntimeTests.fixture
    start_fixture = fixtures.RuntimeTests.start_fixture

    async def websocket(self, request):
        # 每次連線送出相同格式的持續行情，讓測試能偵測不必要的重訂閱。
        from aiohttp import web
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive_json()
        self.connections += 1
        number = 1
        try:
            while not ws.closed:
                _, payload = self.fixture('binance-futures', number)
                await ws.send_json(payload)
                number += 1
                await asyncio.sleep(.03)
        except (ConnectionError, OSError):
            pass
        return ws

    async def asyncSetUp(self):
        # 只有容器程序管理替換為測試後端；交接資料與鎖都使用正式實作。
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / '_state').mkdir()
        self.runner, port = await self.start_fixture()
        self.value, feed = self.settings('binance-futures', port)
        self.feeds = patch.object(fixtures.adapter('binance-futures'), 'feeds', return_value=[feed])
        self.feeds.start()
        self.runtime = fixtures.LocalRuntime(self.root, self.value)
        self.runtime.open = AsyncMock()
        self.runtime.close = AsyncMock()
        self.backend = patch.object(manager, 'Runtime', return_value=self.runtime)
        self.backend.start()
        self.jobs = []

    async def asyncTearDown(self):
        # 任一斷言失敗也停止主管及測試行情，確保資源可重複使用。
        for role, job in reversed(self.jobs):
            role.cancel.set()
            await asyncio.gather(job, return_exceptions=True)
        for worker in list(self.runtime.tasks):
            await self.runtime.stop(worker)
        self.backend.stop()
        self.feeds.stop()
        await self.runner.cleanup()
        self.tmp.cleanup()

    def launch(self, name, fail_proof=False):
        # 同時存在兩個獨立主管，測試以較短的觀察期代替正式的十秒下限。
        args = SimpleNamespace(supervisor_id=name, data_volume=None, host_output=None, backend='docker',
                               image='fixture-image', upgrade_timeout=8, duration=None)
        role = roles.Leadership(self.root, args, self.value)
        role.observe = .25
        if fail_proof:
            original = role.sample
            role.sample = lambda n: None if n == name else original(n)
        task = asyncio.create_task(role.manage(name != 'primary', lambda owner: manager.supervise_generation(role, args, owner)))
        self.jobs.append((role, task))
        return role, task

    async def wait_for(self, condition):
        # 有界等待實際狀態轉移；若程序已失敗，立即顯示例外。
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 12
        while not condition():
            self.assertLess(loop.time(), deadline, '等待主管狀態轉移逾時')
            for _, job in self.jobs:
                if job.done() and not job.cancelled():
                    job.result()
            await asyncio.sleep(.04)

    async def test_two_supervisors_share_continuous_workers_and_one_writer(self):
        # 升級完成才能停舊主管，停止待命程序後新版仍繼續落盤。
        old, old_job = self.launch('primary')
        await self.wait_for(lambda: old.sample('primary') and old.sample('primary')['seq'] > 4)
        first = roles.read(self.root / 'status.json')
        new, new_job = self.launch('next')
        await self.wait_for(lambda: roles.read(old.owner).get('owner') == 'next')
        last = roles.read(self.root / 'status.json')
        self.assertEqual(first['active'], last['active'])
        self.assertTrue(roles.progressed(roles.evidence(first, old.feeds, 20), roles.evidence(last, old.feeds, 20), .1))
        self.assertFalse(old_job.done())
        self.assertTrue(roles.busy(old.writer))
        old.cancel.set()
        await old_job
        await self.wait_for(lambda: roles.read(self.root / 'status.json')['raw_records'] > last['raw_records'])
        self.assertEqual(self.connections, 2)
        new.cancel.set()
        self.assertEqual(await new_job, 0)
        rows = list(fixtures.read_records(self.root))
        ids = [fixtures.atoms(r)[0]['ident'] for r in rows]
        self.assertEqual(len(ids), len(set(ids)))

    async def test_failed_trial_returns_writer_without_draining_workers(self):
        # 候選拿到寫入鎖後仍須以兩路 ACK 證明可用，驗證失敗時舊主管自行恢復。
        old, _ = self.launch('primary')
        await self.wait_for(lambda: old.sample('primary') and old.sample('primary')['seq'] > 4)
        new, new_job = self.launch('next', fail_proof=True)
        await self.wait_for(lambda: roles.read(self.root / 'status.json').get('supervisor_id') == 'next')
        new.cancel.set()
        self.assertEqual(await new_job, 75)
        baseline = roles.read(self.root / 'status.json')['raw_records']
        await self.wait_for(lambda: roles.read(self.root / 'status.json').get('supervisor_id') == 'primary'
                            and roles.read(self.root / 'status.json')['raw_records'] > baseline)
        self.assertEqual(roles.read(old.owner)['owner'], 'primary')
        self.assertEqual(self.connections, 2)
        self.assertFalse(list((self.root / '_state').glob('*.drain')))

    async def test_incompatible_upgrade_leaves_active_writer_in_place(self):
        # 即使同一資料目錄，契約不符也須在舊主管讓位之前拒絕。
        old, _ = self.launch('primary')
        await self.wait_for(lambda: old.owner.exists())
        new, task = self.launch('incompatible')
        new.contract = 'incompatible'
        with self.assertRaisesRegex(ValueError, 'incompatible'):
            await task
        self.assertFalse(old.grant.exists())
        self.assertTrue(roles.busy(old.writer))

    async def test_missing_shared_lock_refuses_before_yield(self):
        # 兩個容器掛載不同鎖位置時，候選須在發出交接請求前拒絕。
        old, _ = self.launch('primary')
        await self.wait_for(lambda: old.owner.exists())
        new, task = self.launch('wrong-mount')
        new.observe_owner = AsyncMock(return_value={'paths': {}})
        new.writer = self.root / 'wrong.lock'
        new.data_lock = new.writer
        with self.assertRaisesRegex(RuntimeError, 'lock is not visible'):
            await task
        self.assertFalse(old.grant.exists())

    async def test_invalid_ipc_does_not_stop_or_ack(self):
        # 未認證錯誤僅關連線，已認證副本的序號缺口只隔離該副本。
        supervisor = manager.Supervisor(self.root, self.value, self.runtime)
        supervisor.state['active'] = ['bad', 'peer']
        supervisor.worker_alive = AsyncMock(return_value=True)
        server = await asyncio.start_unix_server(supervisor.accept, path=str(supervisor.paths['socket']))
        try:
            for malformed in (True, False):
                reader, writer = await asyncio.open_unix_connection(str(supervisor.paths['socket']))
                if malformed:
                    await manager.write_packet(writer, [])
                else:
                    await manager.write_packet(writer, dict(type='hello', worker='bad', identity=supervisor.expected))
                    self.assertTrue((await manager.read_packet(reader))['accepted'])
                    await manager.write_packet(writer, dict(type='batch', batch=[dict(seq=2, record=fixtures.event())]))
                self.assertEqual(await reader.read(), b'')
                writer.close()
                await writer.wait_closed()
            self.assertFalse(supervisor.stop.is_set())
            self.assertEqual(supervisor.store.cursor('bad'), 0)
            self.assertEqual(supervisor.storage_holds(), {'bad'})
        finally:
            server.close()
            await server.wait_closed()
            supervisor.schedule.close()
            supervisor.store.close()

    async def test_damaged_old_spool_cannot_block_replacement(self):
        # 故障副本的 SQLite 損壞時，健康路與替補路仍可交付，保留原件待查。
        supervisor = manager.Supervisor(self.root, self.value, self.runtime)
        stale = supervisor.paths['spool'] / 'failed'
        stale.mkdir(parents=True)
        path = stale / 'spool.sqlite'
        path.write_bytes(b'bad-spool')
        supervisor.state['active'] = ['failed', 'peer']
        supervisor.hold_storage('failed', 'disk I/O error')
        try:
            await supervisor.serve(2)
            info = supervisor.state['storage_incidents']['failed']
            self.assertGreater(supervisor.store.cursor(info['replacement']), 0)
            self.assertGreater(supervisor.store.cursor('peer'), 0)
            self.assertEqual(path.read_bytes(), b'bad-spool')
            self.assertEqual(supervisor.storage_holds(), {'failed'})
        finally:
            supervisor.schedule.close()
            supervisor.store.close()

    async def test_destination_failure_retries_without_worker_ack(self):
        # 正式寫入失敗時主管不給 ACK，同一批重新提交後只產生一列。
        supervisor = manager.Supervisor(self.root, self.value, self.runtime)
        batch = [dict(seq=1, record=fixtures.event())]
        try:
            with patch.object(supervisor.store, 'flush', side_effect=OSError('disk full')):
                self.assertIsNone(supervisor.commit_batch('peer', batch))
            self.assertIsNone(supervisor.commit_batch('peer', batch))
            supervisor.output_retry = 0
            self.assertEqual(supervisor.commit_batch('peer', batch), 1)
            self.assertEqual(supervisor.store.db.execute('SELECT written FROM output').fetchone()[0], 1)
            self.assertIsNone(supervisor.output_fault)
        finally:
            supervisor.store.close()
            supervisor.schedule.close()

    def test_rest_only_shared_lease_allows_idle_peer(self):
        # 純 REST 在同一時段可能只有一個副本領取租約，仍需新回應、耐久 ACK 與正式進度。
        first = dict(paths={w: dict(started=1, sessions={}, ack=4, received=2) for w in ('a', 'b')}, seq=10, at=1)
        last = dict(paths={w: dict(v) for w, v in first['paths'].items()}, seq=11, at=12)
        self.assertFalse(roles.progressed(first, last, 10))
        last['paths']['a'].update(ack=5, received=3)
        self.assertTrue(roles.progressed(first, last, 10))
