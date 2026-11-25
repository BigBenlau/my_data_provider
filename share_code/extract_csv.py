"""解壓所有 exchange，保留來源根目錄以下的完整路徑。

python extract_csv.py
python extract_csv.py --source /path/datasets --output /path/datasets_extracted
"""
import argparse
import gzip
import logging
from itertools import islice
from pathlib import Path
import shutil
import tempfile

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_LINES = 100_000  # 與 head -n 100000 相同，包含表頭。


def extract_gz_files(source_dir, output_dir):
    """遞迴解壓 .csv.gz；原檔保留，成功後才原子替換目的 CSV。

    例如 datasets/binance-futures/trades/2025-10-22_BTCUSDT.csv.gz
    輸出 datasets_extracted/binance-futures/trades/2025-10-22_BTCUSDT.csv。
    同目錄另存 2025-10-22_BTCUSDT_sample.csv，包含表頭共前 100000 行。
    不足 100000 行則全部複製。重跑會覆蓋對應的完整檔及 sample。
    解壓及 sample 暫存完成後才替換既有結果；兩個檔案分別原子替換。
    """
    source = Path(source_dir).resolve()
    output = Path(output_dir).resolve()
    if not source.is_dir():
        raise NotADirectoryError(f"來源資料夾不存在：{source}")
    if output == source or source in output.parents or output in source.parents:
        raise ValueError("來源與輸出必須是分開的資料夾，不能互相包含")
    files = sorted(source.rglob("*.csv.gz"))
    if not files:
        logging.warning("找不到 .csv.gz 檔案：%s", source)
        return 0
    failures = []
    count = 0
    for gz_path in files:
        destination = output / gz_path.relative_to(source).with_suffix("")
        sample_destination = destination.with_name(f"{destination.stem}_sample.csv")
        temporary = None
        sample_temporary = None
        try:
            destination.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(gz_path, "rb") as src:
                with tempfile.NamedTemporaryFile(
                    mode="wb", dir=destination.parent, prefix=".extract-", suffix=".tmp", delete=False
                ) as dst:
                    temporary = Path(dst.name)
                    shutil.copyfileobj(src, dst)
            # 讀取已解壓暫存檔，避免再次解壓；逐行複製保留原始 bytes。
            with temporary.open("rb") as src:
                with tempfile.NamedTemporaryFile(
                    mode="wb", dir=destination.parent, prefix=".sample-", suffix=".tmp", delete=False
                ) as dst:
                    sample_temporary = Path(dst.name)
                    dst.writelines(islice(src, SAMPLE_LINES))
            temporary.replace(destination)
            sample_temporary.replace(sample_destination)
            count += 1
            logging.info("解壓完成：%s", destination)
            logging.info("Sample 完成（含表頭最多 %s 行）：%s", SAMPLE_LINES, sample_destination)
        except Exception:
            failures.append(str(gz_path.relative_to(source)))
            logging.exception("解壓失敗：%s", gz_path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            if sample_temporary is not None:
                sample_temporary.unlink(missing_ok=True)
    if failures:
        raise RuntimeError(f"已成功解壓 {count} 檔，失敗 {len(failures)} 檔：{', '.join(failures)}")
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=BASE_DIR / "datasets")
    parser.add_argument("--output", type=Path, default=BASE_DIR / "datasets_extracted")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    count = extract_gz_files(args.source, args.output)
    logging.info("完成，共解壓 %s 個檔案", count)


if __name__ == "__main__":
    main()
