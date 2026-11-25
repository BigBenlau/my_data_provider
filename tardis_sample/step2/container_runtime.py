"""相同 worker 入口的 Docker 管理與本機測試後端；不負責市場語義。"""
import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import aiohttp
from common import HERE, LOG, atomic_json
from redundancy import code_version, fingerprint
from raw import StorageError


class Runtime:
    """正式使用 Docker；本機模式供無 daemon 的短測與故障注入。"""
    def __init__(self, root, config, backend, image, host_root=None):
        # 程序清單與容器名稱持久化，supervisor 重啟後可重新接管。
        self.root = Path(root); self.config = config; self.backend = backend
        self.requested_image = image; self.host_root = host_root or str(self.root)
        self.path = self.root / '_state' / 'runtime.json'; self.http = None
        self.entries = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.prefix = 'collector-' + config['config_hash'][:8] + '-' + fingerprint(self.host_root)[:6] + '-'

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
        entry = str(HERE / '01_collect.py') if self.backend == 'local' else '/app/step2/01_collect.py'
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
            await self.start_container(worker)
        atomic_json(self.path, self.entries)
        LOG.info('event=worker_started worker=%s backend=%s image=%s', worker, self.backend, self.image)

    async def start_container(self, worker):
        # 掛載同一磁碟及設定；重啟既有容器前重新核對 immutable image。
        name = self.prefix + worker
        old = await self.api('GET', '/containers/' + name + '/json', allowed=(200, 404))
        if old and 'Id' in old:
            if old['Image'] != self.image: raise ValueError('existing worker image mismatch')
            if not old['State']['Running']: await self.api('POST', '/containers/' + name + '/start', allowed=(204, 304))
        else:
            body = dict(Image=self.image, Cmd=self.command(worker, '/data'),
                        Labels={'collector.group': self.config['config_hash'], 'collector.worker': worker},
                        HostConfig={'Binds': [self.host_root + ':/data'], 'RestartPolicy': {'Name': 'unless-stopped'},
                                    'LogConfig': {'Type': 'json-file', 'Config': {'max-size': '10m', 'max-file': '3'}}})
            # 正常 drain 回傳零，不應自動復活；程序崩潰由 Docker 及主管接續恢復。
            body['HostConfig']['RestartPolicy'] = {'Name': 'on-failure', 'MaximumRetryCount': 3}
            await self.api('POST', '/containers/create?name=' + name, body)
            await self.api('POST', '/containers/' + name + '/start')
        self.entries[worker] = dict(name=name, image=self.image)

    async def alive(self, worker):
        # 程序狀態與 IPC 健康分開；活著不代表可安全交接。
        item = self.entries.get(worker)
        if not item: return False
        if item['image'] != self.image: raise ValueError('runtime image changed')
        if self.backend == 'docker':
            info = await self.api('GET', '/containers/' + item['name'] + '/json', allowed=(200, 404))
            if 'Id' not in info: return False
            if info['Image'] != self.image: raise ValueError('worker image mismatch')
            if not info['State']['Running'] and info['State'].get('ExitCode') == 74:
                raise StorageError('worker storage failure; inspect disk and retained spool before restarting')
            return info['State']['Running']
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
