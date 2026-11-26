# Docker 部署採集器

每家交易所由一個 supervisor 管理兩個 worker。五家服務共用映像 `market-collector:local`，使用 `--exchange` 選取協議。建置入口 `collector` 屬於 `build` profile，日常啟動的服務是五個交易所 supervisor。商品與原生 channel 由 `settings.json` 的 `exchanges.<交易所>.markets` 指定。[採集設定](settings.json) · [Compose 服務](compose.yaml)

五家交易所的服務名稱、市場 ID、批量啟停與各自可用的 CSV 格式見 [五家交易所操作指南](../RUN_01_02.md)。

## 從 Git 建置與短測

Git 已保存八個市場的 metadata。建置時 Docker 會將此版本的 `tardis_sample/exchange_info/` 複製到映像的 `/app/exchange_info/`。新工作目錄可直接建置。需要更新來源版本時才從 `tardis_sample/step2/01_collector/` 執行 `python3 ../../download_exchange_info.py`，核對 JSON 差異後再提交。[採集用交易所 metadata](../../exchange_info/)

```bash
# 從 Git 專案根目錄進入部署目錄，這次短測使用獨立輸出路徑。
cd tardis_sample/step2/01_collector
export DATA_ROOT="$PWD/output/raw/smoke-001"
# 選一個服務名稱：binance、bybit、okx、coinbase 或 upbit。
export EXCHANGE=okx

# 檢查 Compose 設定並建立共用映像。
docker compose config --quiet
docker compose build collector

# 先做單組短測，worker 會隨主管正常結束而排空停止。
docker compose run --rm "$EXCHANGE" \
  python /app/step2/01_collector/01_collect.py cluster --exchange "$EXCHANGE" \
  --output /data --host-output "$DATA_ROOT/$EXCHANGE" --duration 30
```

需要固定 Python 基底版本時，可在建置命令加入 `--build-arg PYTHON_IMAGE=python@sha256:<digest>`，並將 `<digest>` 換成部署使用的映像摘要。

## 啟動與停止正式採集

沿用上一步的工作目錄。啟動單一交易所可指定服務名稱，啟動全部五家則使用 `docker compose up -d`。

```bash
# 正式批次使用自己的輸出路徑。
export DATA_ROOT="$PWD/output/raw/run-001"
docker compose up -d "$EXCHANGE"

docker compose logs -f "$EXCHANGE"
# 只列出這家交易所的 worker，包含換代期間的候選者。
docker ps --filter "label=collector.exchange=$EXCHANGE" --format '{{.Names}}'
# 正常停止會要求 worker 停止收訊、排空交付後移除容器。
docker compose stop "$EXCHANGE"
```

`DATA_ROOT` 必須是 Docker 主機的絕對路徑，並使用本機磁碟。容器內是 `/data`。`--host-output` 供主管建立 worker 的相同 bind mount。SQLite WAL 不支援把本資料目錄放在 NFS 等網路檔案系統。Docker socket 只掛載主管，具有主機容器管理權限，應部署在可信的專用主機。

新 worker 的 Docker 名稱是 `collector-<交易所>-<批次識別>-<worker ID>`。六碼批次識別來自資料目錄或 volume 名稱，同批次各副本的最後一段不同。主管把實際名稱寫在該批次的 `_state/runtime.json`，重啟可依登記名稱接管。現有容器沿用建立時的名稱與標籤，新版批次建立的 worker 會帶有 `collector.exchange` 標籤。

主管啟動時解析本機映像的 immutable SHA-256 image ID。換代一律使用該 ID，不執行 pull。不符合映像、程式雜湊或採集設定雜湊的 worker 拒絕加入。程式升級與設定變更使用新的輸出 run。不要在已有資料目錄上混用不同版本。

主管使用 `unless-stopped`。worker 使用 `on-failure`，正常 drain 不會被 Docker 自動復活。主管接管意外退出的 worker 時沿用原身分與 spool。儲存故障會停止並報錯，應檢查磁碟及保留的 spool，不可為重啟而刪掉未確認資料。

如需代理，在設定中明確指定容器可達的代理 URL。容器不會自動取得主機終端的代理環境。`localhost` 在容器內指向該容器。

## 處理舊版主管反覆重啟與 worker 儲存錯誤

若 traceback 結束於 `container_runtime.alive()` 的 `worker storage failure`，舊版主管讀到了某一個 worker 的退出碼 74。先在故障 Docker 主機上查 worker 原始例外。舊容器也有 `collector.worker` 標籤，可用日誌中的 worker ID 定位。下列命令唯讀，兩個 ID 請按當次事故替換。

```bash
for worker in 1765a1759585 7ac39d659b9d; do
  for cid in $(docker ps -aq --filter "label=collector.worker=$worker"); do
    docker inspect -f '{{.Name}} status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} restarts={{.RestartCount}}' "$cid"
    docker logs --tail 500 "$cid" 2>&1 | grep -Ei -A12 'spool_failed|worker_storage_failed|exceeds capacity|free disk|memory byte|disk I/O|database or disk is full'
  done
done
docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' step2-binance-1
```

在 Linux 主機用 `df -h <上一步的路徑>` 和 `df -i <上一步的路徑>` 檢查可用空間及 inode。`unacknowledged spool exceeds capacity` 表示單路待 ACK 的 payload 達到配置上限，`spool memory byte limit exceeded` 是尚未寫入磁碟的佇列達上限，`free disk below configured reserve` 是實際可用磁碟低於保留量。SQLite I/O 錯誤另查檔案系統及核心日誌。

保留舊 supervisor、worker 的日誌、映像 ID，以及完整批次（含 `_spool/`、`_state/`、SQLite WAL、`raw/`、`manifest.json`）。健康 worker 可能仍在寫入；備份用整個資料卷的同一時點快照，或在確認這一組的所有寫入程序都停止後複製整個目錄。舊版主管獨自停止時 worker 仍可收訊。換新版採集使用新的批次目錄。

### 從凍結快照離線復原已落盤資料

以下在 `01_collector/` 執行。`SNAPSHOT_DIR` 指向上述不可變快照裡的單一交易所批次，`RESTORE_PARENT` 指向另一個有足夠容量的磁碟目錄。容量須能容納整份工作副本與新增的合併索引、JSONL。程式從原版映像載入當時的提交邏輯，核對 manifest 的程式、映像與設定身分，在 `recovered/` 建立獨立副本。若複製已完成而重播中斷，使用相同快照和目的地重試會依已提交游標接續。若初次複製中斷，另選一個空目的地。

```bash
OLD_IMAGE=$(docker inspect -f '{{.Image}}' step2-binance-1)
SNAPSHOT_DIR=/mnt/snapshots/run-001/binance
RESTORE_PARENT=/mnt/recovery/binance-001
mkdir -p "$RESTORE_PARENT"
docker run --rm --pull=never --network=none -e PYTHONPATH=/app/step2 \
  --mount "type=bind,source=$SNAPSHOT_DIR,target=/snapshot,readonly" \
  --mount "type=bind,source=$RESTORE_PARENT,target=/restore" \
  --mount "type=bind,source=$PWD/ops/recover_spool_copy.py,target=/tmp/recover.py,readonly" \
  "$OLD_IMAGE" python /tmp/recover.py --snapshot /snapshot --output /restore/recovered --image-id "$OLD_IMAGE"
```

`recovered/spool-recovery.json` 列出每個 worker 原有 ACK、主管提交游標與 worker 已持久化的末序號。完成條件是每條路的提交游標均到達該路已落盤的末序號。可對 `recovered/` 執行 `01_collect.py export-csv` 匯出封段。原 manifest 保留事故當時狀態。新補交的尾段按 worker ID、各路的交付序號重播；跨路先後已無法還原。仍在記憶體的資料、斷線期間的行情及已觀察到的市場缺口，須獨立核對。

新版主管會在 `status.json.storage_blocked_workers` 列出退出 74 的 worker，保留其容器和 spool，同時讓可用副本繼續交付。兩路受阻時主管仍可提供診斷，行情採集可能已全面中斷。正常關停時如有待查的故障 worker，manifest 標為 `storage_blocked`。

## 長期運行時如何判斷阻塞

每家主管的 `status.json.delivery_rates` 按 worker 提供相鄰兩份有效回報的速率。先看同一條路數個連續心跳，並與 `workers` 中的 `pending_bytes`、`oldest_pending_s`、`memory_bytes` 交叉核對。重啟或收不到新心跳時，速率欄位可能是空物件。

| 現象 | 判讀與處理 |
|---|---|
| `enqueued_per_s` 持續高於 `ack_per_s`，且 `pending_bytes_per_s` 為正、最舊資料越等越久 | 此 worker 的輸入持續快於交付。比較 `batch_metrics.index_ms` 與 `persist_ms`，同時檢查主機 CPU、資料盤延遲、空間與 inode。資料庫提交或磁碟飽和時，先將匯出及 02／03 的大量讀寫移到其他時段或磁碟。 |
| 積壓開始下降，`seconds_to_limit` 為空 | ACK 正在追趕或積壓持平。此估算只在位元組增加時依當前斜率外推，行情量改變時會隨之變動。 |
| 出現 `event=spool_waiting_for_ack` | 寫入執行緒已讓出鎖，讓 ACK 清理空間。主管仍可交付；監測 `workers.<id>.spool_capacity_waits` 有沒有持續增加。 |
| `exit_code=74`，兩家同一時段受影響 | 分別查兩家的 worker 原始例外及 `/data` 實際掛載。若共用資料盤，檢查可用空間和寫入延遲。Supervisor 的 `rotation_preflight ... action=retain_workers` 是嚴格交接政策的通知。 |

`redundancy.spool_capacity_wait_seconds` 預設為 10 秒，可於各交易所的 `exchanges.<交易所>.redundancy` 設定；0 表示邏輯容量不足立即報錯。這段時間只用於等待已落盤資料的 ACK。單批超過硬上限、記憶體額度耗盡或實際儲存錯誤仍會報錯。持續過載應提升提交吞吐或分散負載，增大 spool 只延後到達上限的時間。

worker 會在寫入與確認資料時，同步保存待交付的位元組總量。重新啟動時，若游標吻合即可使用該計數。舊版 spool 第一次由新版開啟，或離線復原改變了游標時，會掃描尚待交付的資料一次。退役 worker 則分批排空，活躍 worker 可在批次之間繼續提交。

舊版 worker 的容器名稱可能只有雜湊。在故障 Linux 主機執行下列唯讀命令，可按批次查 OKX、Bybit 的原始錯誤及資料盤。若 Docker 使用遠端 daemon，這些資料路徑位於 daemon 主機。

```bash
for exchange in okx bybit; do
  run=$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' "step2-${exchange}-1") || continue
  echo "$exchange $run"
  df -h "$run"
  df -i "$run"
  python3 -c 'import json,sys; print(*(v["name"] for v in json.load(open(sys.argv[1])).values() if "name" in v), sep="\n")' "$run/_state/runtime.json" |
  while IFS= read -r worker; do
    [ -n "$worker" ] || continue
    docker inspect -f '{{.Name}} state={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' "$worker"
    docker logs --tail 500 "$worker" 2>&1 | grep -Ei -A12 'spool_failed|worker_storage_failed|exceeds capacity|free disk|memory byte|disk I/O|database or disk is full'
  done
done
```

## 使用 Docker 原生 volume

Docker Desktop 的主機共享目錄可能影響 SQLite 同步寫入。本次另以原生 volume 完成相同來源重播。原有 bind mount 仍可使用，程式不會遷移既有資料。原生 volume 由 Docker 管理，可供主管及 worker 共用並在容器停止後保存，詳見 [Docker volume 文件](https://docs.docker.com/engine/storage/volumes/)。每個交易所、每個新 run 使用獨立 volume。

```bash
# 從本目錄建置相同映像，再以新 volume 做單組短測。
docker build -f ../Dockerfile -t market-collector:local ../..
export EXCHANGE=upbit
export COLLECTOR_VOLUME="collector-${EXCHANGE}-run-002"
docker volume create "$COLLECTOR_VOLUME"
docker run --rm --init --name "collector-${EXCHANGE}-short-test" \
  --stop-timeout 90 \
  -v "$COLLECTOR_VOLUME:/data" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  market-collector:local python /app/step2/01_collector/01_collect.py cluster \
  --exchange "$EXCHANGE" --output /data --data-volume "$COLLECTOR_VOLUME" \
  --duration 30
```

`--data-volume` 必須與主管實際掛載於 `/data` 的 volume 相同。與 `--host-output` 互斥。其他交易所替換 `--exchange`、容器名稱及 volume 名稱即可，worker 程式完全相同。正式採集才移除 `--duration` 並使用背景執行及重啟政策。不可同時啟動兩個主管寫同一 volume。

停止並排空後，五家交易所都可掛載各自的 volume 匯出原生 channel CSV。各家批次可使用同一映像中的 `/app/step2/02_normalize_tardis/02_normalize_tardis.py --exchange <交易所>` 離線正規化，再將該家的輸出交給第 03 階段對帳。備份需包含 JSONL、manifest 及狀態。只取 channel CSV 不足以恢復採集。不要刪除尚未備份的 volume，也不要以執行中資料的普通檔案複製代替一致性備份。
