"""相同 worker 入口的 Docker 管理與本機測試後端；不負責市場語義。"""
import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import aiohttp
from common import LOG, atomic_json
from .transport import COLLECTOR_ROOT
from .redundancy import code_version, fingerprint
from .raw import StorageError


class WorkerStorageFailure(StorageError):
    """已停止的 worker 回報儲存故障，主管仍可處理其他副本的交付。"""


class Runtime:
    """正式使用 Docker；本機模式供無 daemon 的短測與故障注入。"""
    def __init__(self, root, config, backend, image, host_root=None):
        # 程序清單與容器名稱持久化，supervisor 重啟後可重新接管。
        self.root = Path(root); self.config = config; self.backend = backend
        self.requested_image = image; self.host_root = host_root or str(self.root)
        self.path = self.root / '_state' / 'runtime.json'; self.http = None
        self.entries = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.prefix = f"collector-{config['family']}-{fingerprint(self.host_root)[:6]}-"

    async def open(self):
        # 只解析本機既有映像，不拉取 tag；之後一律使用不可變 image ID。
        if self.backend == 'local':
            self.image = 'local:' + code_version(); return
        options = self.config['redundancy']
        connector = aiohttp.UnixConnector(path=options['docker_socket'])
        self.http = aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=30))
        info = await self.api('GET', '/images/' + self.requested_image + '/json')
        self.image = info['Id']

    async def api(self, method, path, body=None, allowed=(200, 201, 204)):
        # Docker socket 只掛載 supervisor；worker 沒有容器管理權限。
        url = 'http://docker/' + self.config['redundancy']['docker_api'] + path
        async with self.http.request(method, url, json=body) as response:
            text = await response.text()
            if response.status not in allowed: raise RuntimeError(f'Docker {method} {path}: {response.status} {text}')
            return json.loads(text) if text else None

    def command(self, worker, root):
        # 所有執行個體使用同一命令，只有執行期 worker 識別與資料路徑不同。
        entry = str(COLLECTOR_ROOT / '01_collect.py') if self.backend == 'local' else '/app/step2/01_collector/01_collect.py'
        return ['python' if self.backend == 'docker' else sys.executable, '-u', entry, 'worker',
                '--root', root, '--worker-id', worker, '--image', self.image]

    async def start(self, worker):
        # 重啟同個 worker 可重送舊 spool；新的候選者才會分配新的名稱。
        drain = self.root / '_state' / (worker + '.drain')
        drain.unlink(missing_ok=True)
        if self.backend == 'local':
            handle = (self.root / 'worker-runtime.log').open('a')
            try: child = subprocess.Popen(self.command(worker, str(self.root)), stdout=handle, stderr=handle, start_new_session=True)
            finally: handle.close()
            self.entries[worker] = dict(pid=child.pid, image=self.image)
        else:
            try:
                await self.start_container(worker)
            except WorkerStorageFailure:
                # 主管若在建立容器後崩潰，接管時也要記住已退出的同一副本。
                atomic_json(self.path, self.entries)
                raise
        atomic_json(self.path, self.entries)
        LOG.info('event=worker_started worker=%s backend=%s image=%s', worker, self.backend, self.image)

    async def start_container(self, worker):
        # 掛載同一磁碟及設定；重啟既有容器前重新核對 immutable image。
        name = self.prefix + worker
        # 舊版批次保留已登記的名稱，讓既有容器能正常接管及排空。
        recorded = self.entries.get(worker, {}).get('name')
        if recorded and recorded != name:
            previous = await self.api('GET', '/containers/' + recorded + '/json', allowed=(200, 404))
            if previous and 'Id' in previous:
                return await self.resume_container(worker, recorded, previous)
        old = await self.api('GET', '/containers/' + name + '/json', allowed=(200, 404))
        if old and 'Id' in old:
            return await self.resume_container(worker, name, old)
        body = dict(Image=self.image, Cmd=self.command(worker, '/data'),
                    Labels={'collector.group': self.config['config_hash'], 'collector.worker': worker,
                            'collector.exchange': self.config['family']},
                    HostConfig={'Binds': [self.host_root + ':/data'],
                                'RestartPolicy': {'Name': 'on-failure', 'MaximumRetryCount': 3},
                                'LogConfig': {'Type': 'json-file', 'Config': {'max-size': '10m', 'max-file': '3'}}})
        await self.api('POST', '/containers/create?name=' + name, body)
        await self.api('POST', '/containers/' + name + '/start')
        self.entries[worker] = dict(name=name, image=self.image)

    async def resume_container(self, worker, name, info):
        # 核對映像和既有標籤，避免接管其他批次的 worker。
        if info['Image'] != self.image:
            raise ValueError('existing worker image mismatch')
        labels = info['Config']['Labels']
        if labels.get('collector.group') != self.config['config_hash'] or labels.get('collector.worker') != worker:
            raise ValueError('existing worker identity mismatch')
        self.entries[worker] = dict(name=name, image=self.image)
        if not self.running(worker, name, info):
            await self.api('POST', '/containers/' + name + '/start', allowed=(204, 304))

    @staticmethod
    def running(worker, name, info):
        # 接管與健康檢查共用退出判定，避免從另一條啟動路徑重啟故障副本。
        state = info['State']
        if (not state['Running'] or state.get('Restarting')) and state.get('ExitCode') == 74:
            raise WorkerStorageFailure(f'worker={worker} container={name} exit_code=74')
        return state['Running']

    async def alive(self, worker):
        # 程序狀態與 IPC 健康分開；活著不代表可安全交接。
        item = self.entries.get(worker)
        if not item: return False
        if item['image'] != self.image: raise ValueError('runtime image changed')
        if self.backend == 'docker':
            info = await self.api('GET', '/containers/' + item['name'] + '/json', allowed=(200, 404))
            if 'Id' not in info: return False
            if info['Image'] != self.image: raise ValueError('worker image mismatch')
            return self.running(worker, item['name'], info)
        result = await asyncio.to_thread(subprocess.run, ['ps', '-p', str(item['pid']), '-o', 'command='], capture_output=True, text=True)
        return result.returncode == 0 and '--worker-id ' + worker in result.stdout

    async def stop(self, worker):
        # 呼叫前由 supervisor 驗證已排空；即使強制停止也不刪除未確認 spool。
        item = self.entries.get(worker)
        if not item: return
        if self.backend == 'docker':
            await self.api('POST', '/containers/' + item['name'] + '/stop?t=10', allowed=(204, 304, 404))
            await self.api('DELETE', '/containers/' + item['name'], allowed=(204, 404))
        elif await self.alive(worker):
            os.kill(item['pid'], signal.SIGTERM)
            for _ in range(50):
                if not await self.alive(worker): break
                await asyncio.sleep(.2)
            if await self.alive(worker): os.kill(item['pid'], signal.SIGKILL)
        self.entries.pop(worker, None); atomic_json(self.path, self.entries)

    async def close(self):
        # 關閉管理連線不停止 worker，讓 supervisor 崩潰或更新時仍可採集。
        if self.http: await self.http.close()
