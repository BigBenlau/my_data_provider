#!/usr/bin/env python3
"""第 02 階段：按交易所選擇 mapper，離線轉換原始 JSONL。"""
from pathlib import Path
import sys

# 直接以腳本執行時，以腳本位置找到 Step2 共用套件。
if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    __package__ = Path(__file__).resolve().parent.name

from .normalization import cli


if __name__ == '__main__':
    cli()
