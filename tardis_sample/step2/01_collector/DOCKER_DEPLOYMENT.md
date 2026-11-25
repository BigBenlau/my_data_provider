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
