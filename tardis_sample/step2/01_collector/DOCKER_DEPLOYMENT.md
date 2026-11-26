# Docker 部署採集器

每家交易所由一個 supervisor 管理兩個 worker。五家服務共用映像 `market-collector:local`，使用 `--exchange` 選取協議。建置入口 `collector` 屬於 `build` profile，日常啟動的服務是五個交易所 supervisor。商品與原生 channel 由 `settings.json` 的 `exchanges.<交易所>.markets` 指定。[採集設定](settings.json) · [Compose 服務](compose.yaml)

五家交易所的服務名稱、市場 ID 和批量啟停指令見 [01 採集指南](README.md)。各市場可產生的 Tardis CSV 類型見 [02 正規化指南](../02_normalize_tardis/README.md#各交易所的輸出)。

## 從 Git 建置與短測

Git 已保存八個市場的 metadata。建置時 Docker 會將此版本的 `tardis_sample/exchange_info/` 複製到映像的 `/app/exchange_info/`。新工作目錄可直接建置。需要更新來源版本時才從 `tardis_sample/step2/01_collector/` 執行 `python3 ../../download_exchange_info.py`，核對 JSON 差異後再提交。[採集用交易所 metadata](../../exchange_info/)

```bash
# 從 Git 專案根目錄進入部署目錄，這次短測使用獨立輸出路徑。
cd tardis_sample/step2/01_collector
DATA_ROOT=$(python3 01_collect.py new-run)
export DATA_ROOT
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

`new-run` 使用主機上的專案 Python 環境，只建立一個由 UTC 時戳和隨機後綴命名的批次目錄，並印出絕對路徑。可用 `--output-root /mnt/hot/market-data` 選擇磁碟。用 `RUN_ID="${DATA_ROOT##*/}"` 取得批次 ID，供後續匯出或對帳命名。

## 啟動與停止正式採集

沿用上一步的工作目錄。啟動單一交易所可指定服務名稱，啟動全部五家則使用 `docker compose up -d`。

```bash
# 正式批次使用自己的輸出路徑。
DATA_ROOT=$(python3 01_collect.py new-run)
export DATA_ROOT
docker compose up -d "$EXCHANGE"

docker compose logs -f "$EXCHANGE"
# 只列出這家交易所的 worker，群組換線時容器名稱維持相同。
docker ps --filter "label=collector.exchange=$EXCHANGE" --format '{{.Names}}'
# 正常停止會要求 worker 停止收訊、排空交付後移除容器。
docker compose stop "$EXCHANGE"
```

`DATA_ROOT` 必須是 Docker 主機的絕對路徑，並使用本機磁碟。容器內是 `/data`。`--host-output` 供主管建立 worker 的相同 bind mount。SQLite WAL 不支援把本資料目錄放在 NFS 等網路檔案系統。Docker socket 只掛載主管，具有主機容器管理權限，應部署在可信的專用主機。

只在開新批次時執行 `new-run`。同一批次的重啟、停止和主管交接繼續使用原本的 `DATA_ROOT`。如果已換終端，可以從現有容器的 `/data` 掛載還原路徑。

新 worker 的 Docker 名稱是 `collector-<交易所>-<批次識別>-<worker ID>`。六碼批次識別來自資料目錄或 volume 名稱，同批次各副本的最後一段不同。主管把實際名稱寫在該批次的 `_state/runtime.json`，重啟可依登記名稱接管。現有容器沿用建立時的名稱與標籤，新版批次建立的 worker 會帶有 `collector.exchange` 標籤。

主管啟動時解析本機映像的 immutable SHA-256 image ID。換代一律使用該 ID，不執行 pull。不符合映像、程式雜湊或採集設定雜湊的 worker 拒絕加入。只更新相容的主管管理程式，可依下節在同一 run 交接。變更 worker、原始資料協議、儲存實作或採集設定時，使用新 run。

主管使用 `unless-stopped`。worker 使用 `on-failure`，正常 drain 不會被 Docker 自動復活。主管接管意外退出的 worker 時沿用原身分與 spool。單一副本報錯時，主管記錄根因、隔離故障程序並以新 ID 補位。確認舊程序停止後，主管以小批次自動補交其已落盤資料；損壞的 spool 留存待查。

如需代理，在設定中明確指定容器可達的代理 URL。容器不會自動取得主機終端的代理環境。`localhost` 在容器內指向該容器。

## 讓兩個版本的主管交接

適用於已由支援交接的版本啟動、且已有 `_state/owner.json` 的批次。一個正式寫入鎖保護 JSONL、索引與 REST 排程。候選主管沿用原 A/B 程序、行情連線及固定的 worker 映像。只有主管管理程式及其相容執行環境可以獨立換版。第一次從沒有交接協議的舊映像部署時，請保留舊批次，以維護窗口正常排空並用新 run 啟動基線版本；舊映像收到正常停止訊號會同步排空其 workers。

從本目錄在同一台 Docker 主機執行。下例針對 Binance，其他四家只需換交易所和原主管名稱。候選從原容器取得批次路徑並沿用掛載。若原服務使用自訂 `--config`，候選也傳相同選項及可見檔案。下列範例適用於本專案 Compose 的 bind mount 部署。

```bash
export EXCHANGE=binance
export OLD=step2-binance-1
export RELEASE=r2
export NEW="supervisor-${EXCHANGE}-${RELEASE}"

# 核對原主管已支援交接，並取得現有批次在主機上的路徑。
docker exec "$OLD" python /app/step2/01_collector/01_collect.py cluster --help |
  grep -F -- '--takeover' >/dev/null || exit 1
docker exec "$OLD" test -s /data/_state/owner.json || exit 1
HOST_RUN=$(docker inspect -f '{{range .Mounts}}{{if and (eq .Destination "/data") (eq .Type "bind")}}{{.Source}}{{end}}{{end}}' "$OLD")
test -n "$HOST_RUN" || exit 1

# 以獨立標籤建置新版主管，保留原 worker 映像供該批次替補。
docker build -f ../Dockerfile -t "market-collector:${RELEASE}" ../..
IMAGE_ID=$(docker image inspect -f '{{.Id}}' "market-collector:${RELEASE}")

# 共用原容器的行情目錄、Docker socket 與主管鎖 volume。
# 先驗證候選。接手成功後才啟用自動重啟。
docker run -d --init --restart=no --stop-timeout 90 --name "$NEW" \
  --log-opt max-size=10m --log-opt max-file=3 \
  --volumes-from "$OLD" -e TZ=Asia/Shanghai \
  -e SUPERVISOR_LOCK_ROOT=/var/lib/collector-locks "$IMAGE_ID" \
  python /app/step2/01_collector/01_collect.py cluster \
  --exchange "$EXCHANGE" --output /data --host-output "$HOST_RUN" \
  --supervisor-id "$RELEASE" --takeover --upgrade-timeout 120

docker logs -f "$NEW"
```

候選先唯讀觀察至少 10 秒。兩個 worker 均需持續收訊及確認提交，所有配置的 WS 須在原 session 保持訂閱，且無已知未覆蓋缺口。純 REST 設定則驗證兩個程序持續運行，且共用排程有新回應、ACK 與正式輸出。舊主管讓出寫入鎖後，worker 維持 WS 並把行情暫存於 spool。新版接手後需再次看到正式序號和兩個 worker 的 ACK 前進、各自接回資料與控制連線、原 session 持續，才寫入新所有權並記錄 `event=supervisor_promoted`。舊主管留在待命狀態。驗證逾時或候選崩潰時，只要舊主管仍在運行，就會接回寫入鎖和待交付資料。

核對成功後可停止待命容器。以後查看日誌、停止或重新啟動該交易所時，操作目前的主管容器；下一次交接以它作為 `OLD`，並使用新的 `RELEASE`。

```bash
docker exec "$NEW" python -c 'import json, sys; sys.exit(json.load(open("/data/_state/owner.json"))["owner"] != sys.argv[1])' "$RELEASE" &&
  docker update --restart unless-stopped "$NEW" &&
  docker stop --time 90 "$OLD"
```

Compose 主管共用 Docker 原生的 `supervisor_locks` volume，並按交易所與批次區分鎖。請在整個 run 期間保留這個 volume。候選若看不到舊主管持有的鎖會在交接前報錯。使用 Docker 資料 volume 時，雙方傳入相同的 `--data-volume` 替代 `--host-output`。直接對運行中的服務執行 `docker compose up` 重建容器會走一般停止流程；需要不中斷換主管時請使用上述交接命令。

### 如何判讀 worker 故障

`worker_quarantined` 會記錄 worker ID、例外原因、可取得的 worker 錯誤與 Docker 退出狀態。`worker_replaced` 記錄新 ID，`worker_spool_recovered` 表示舊 spool 的持久序號已與主管游標核對並完成清理。第一次故障立即補位，同席位短時間內反覆失敗時逐步退避，最多 60 秒。Docker API 暫時失聯時先重試，以免同時建立過多副本。

`status.json` 的 `active`、`supervisor_id` 和 `storage_incidents` 可核對目前的雙路、主管及歷史故障。`commit_blocked` 有內容代表正式儲存暫時拒寫，worker 會保留未確認資料並重試。主管及 worker 共用的主機、磁碟、網路故障仍會影響持續採集，能承受的中斷時間受 spool 容量限制。

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

五家交易所都可唯讀掛載各自的 volume，使用 `export-jsonl --date` 或 `export-csv --date` 匯出前一天的封段。採集期間可對 02 指定同一 UTC 日期；整批重播待停止並排空後執行。各家批次可使用同一映像中的 `/app/step2/02_normalize_tardis/02_normalize_tardis.py --exchange <交易所>` 離線正規化，再將該家的輸出交給第 03 階段對帳。備份需包含 JSONL、manifest 及狀態。只取 channel CSV 不足以恢復採集。不要刪除尚未備份的 volume，也不要以執行中資料的普通檔案複製代替一致性備份。

## 監控群組換線及資料盤

每家主管的持久日誌在 `$DATA_ROOT/<交易所>/supervisor.log`，預設每五秒寫心跳。這個路徑可跨主管接管繼續追蹤。容器日誌和目前的交接狀態可用以下指令讀取。

```bash
cd tardis_sample/step2/01_collector
EXCHANGE=binance
docker compose logs -f --since=15m "$EXCHANGE"
tail -F "$DATA_ROOT/$EXCHANGE/supervisor.log"
jq '{active,ws_rotation,delivery_backlog,delivery_rates,storage,uncovered_gaps}' "$DATA_ROOT/$EXCHANGE/status.json"
docker ps --filter "label=collector.exchange=$EXCHANGE" --format '{{.Names}}'
```

`event=ws_rotation` 記錄候選開線、核准、成功及跳過原因，`ws_rotation.trial` 含群組、副本、舊 session、驗證狀態及期限。最近 100 個決策在 `ws_rotation.history`，完整決策留在 `_state/merge.sqlite` 的 `rotation_decisions`。有證據的收訊停滯、序號缺口及無效行情優先開候選，核准關舊線的成功交接仍至少相隔 600 秒。Upbit 的握手採錯峰發起，HTTP 429 的冷卻由同一交易所 A/B 共用。實際網路中斷及重連可在 `event=replica_notice` 查到，各線最近的連線事件在 `storage.recent_connections`。升級後可用 `docker logs -f <現役主管名稱>` 追蹤新容器。

`delivery_backlog` 在任一路最舊等待超過 10 秒或使用達 80% 時啟動，整理上限降為每批 256 筆。連續三次健康回報均低於 5 秒及 60% 才解除。每路峰值取自相鄰心跳的實際 spool 寫入位元組差。可承受停頓時間採剩餘 spool 容量除以觀測峰值。磁碟剩餘天數以至少一小時、最多最近一天的裝置可用空間淨變化估算，樣本不足或淨成長為零時顯示 `null`。

## 選用第二顆本機磁碟

新去重版本請指定全新的 `DATA_ROOT`。第二顆本機磁碟以獨立檔案系統掛載。先在 Linux 核對裝置及掛載點，再在冷盤建立一次持久 UUID。保留此標識並備份；已存在時沿用原值。

```bash
DATA_ROOT=$(python3 01_collect.py new-run --output-root /mnt/hot/market-data)
export DATA_ROOT
export COLD_ROOT=/mnt/cold
findmnt --mountpoint "$COLD_ROOT"
lsblk -o NAME,PKNAME,SERIAL,FSTYPE,MOUNTPOINTS
if [ ! -e "$COLD_ROOT/.collector-volume-id" ]; then
  mountpoint -q "$COLD_ROOT" && (set -C; uuidgen > "$COLD_ROOT/.collector-volume-id")
fi
export COLD_VOLUME_ID="$(cat "$COLD_ROOT/.collector-volume-id")"
export COMPOSE_FILE=compose.yaml:compose.cold.yaml
docker compose --profile build build collector
docker compose up -d binance bybit okx coinbase upbit
```

冷盤只掛至 supervisor 的 `/cold`。掛載覆寫禁止 Docker 自動建立來源目錄。程式檢查裝置號與身分檔；實體硬碟是否獨立請依上方序號核對。封存目的地為 `$COLD_ROOT/<交易所>/<批次識別>/`，封段滿 24 小時後校驗壓縮檔 SHA-256、逐筆 seq、首末序號及筆數，提交位置目錄後才清理熱盤。冷盤另存 manifest 及永久精確索引。未設定冷盤時，行情及歷史查重索引持續保存在原資料目錄。

冷盤失聯時 `storage.cold_error`、`storage.history_waiting` 及日誌說明狀況。可能命中歷史索引的原文在未確認 spool 等待，其他來源可繼續提交。重掛原裝置後，主管重新核實索引並推進連續 ACK。持續觀察 `oldest_pending_s` 與剩餘容量。兩顆磁碟都要有獨立備份。

在主機執行 01 匯出或 02 時，將 `COLD_ROOT` 設為主機的掛載路徑，原始輸入仍指定熱盤的 run。用 Docker 離線處理時加上冷盤唯讀掛載並設定容器內的路徑。保留熱盤的 manifest、位置目錄及狀態資料庫。

```bash
EXCHANGE=bybit
python3 01_collect.py export-csv --input "$DATA_ROOT/$EXCHANGE" --output "/mnt/analysis/raw-csv/$EXCHANGE"
python3 ../02_normalize_tardis/02_normalize_tardis.py --exchange "$EXCHANGE" --input "$DATA_ROOT/$EXCHANGE" --output "/mnt/analysis/tardis/$EXCHANGE"
```
