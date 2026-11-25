# Step 1 工作目錄

## 報告

- [step1_report.md](step1_report.md)：最新中文報告，為編輯依據。
- [step1_report_en.md](step1_report_en.md)：與中文內容對齊的英文報告。
- [step1_report_en.pdf](step1_report_en.pdf)：保留的英文 PDF 匯出檔，修改 Markdown 後須另行重新匯出。
- [AGENTS.md](AGENTS.md)：報告編輯規則；先修改最新中文，再同步英文。

## 分析與內部產出

- `analyze_dataset_csv.py`：CSV 分析入口。
- `step1_empirical.py`：完整日 BBO、逐欄變更頻率及 gap 分析。
- `requirements-analysis.txt`：分析依賴。
- `research_results/`：實證統計與間隔明細。
- `dataset_reports/`：逐市場、逐 symbol 的分析報告。
- `dataset_reports_bak/`：既有分析報告備份。
- `render_step1_reports.py`：歷史報告模板，未包含後續手動修改；不要執行它覆蓋正式報告。

共用資料與下載工具仍位於上層：`../datasets/`、`../datasets_extracted/`、`../exchange_info/`、`../download_csv.py`、`../download_exchange_info.py`、`../extract_csv.py`。

以下命令從專案根目錄執行：

```sh
python3 -m pip install -r tardis_sample/step1/requirements-analysis.txt
python3 tardis_sample/step1/analyze_dataset_csv.py
python3 tardis_sample/step1/analyze_dataset_csv.py --empirical --source tardis_sample/datasets --output tardis_sample/step1/research_results --date 2026-09-18
```

預設分析來源為上層 `datasets_extracted/`，一般報告預設寫入本目錄的 `dataset_reports/`。上述完整日命令的內部產出寫入 `research_results/empirical/`。
