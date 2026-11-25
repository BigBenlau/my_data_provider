"""下載 EXCHANGE_CONFIG 全部市場的 metadata，僅篩選兩個 symbol 陣列。

python3 tardis_sample/download_exchange_info.py
python3 tardis_sample/download_exchange_info.py --output tardis_sample/exchange_info
JSON 內容一致指解析後的結構和值一致；縮排可能與 HTTP 原文不同。
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import json
import logging
from pathlib import Path
import tempfile
from urllib.request import Request, urlopen

from download_csv import EXCHANGE_CONFIG

OUTPUT_DIR = Path(__file__).resolve().parent / 'exchange_info'


def parse_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f'Duplicate JSON key: {key}')
            result[key] = value
        return result
    def invalid(value):
        raise ValueError(f'Invalid JSON number: {value}')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def canonical(value):
    # 保留 JSON 型別差異，例如 true 與 1、1 與 1.0。
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def filter_symbols(original, symbols):
    if not symbols or not all(isinstance(s, str) and s for s in symbols):
        raise ValueError('symbols 必須是非空字串清單')
    wanted = {s.casefold() for s in symbols}
    result = deepcopy(original)
    for container, key in ((result, 'availableSymbols'), (result['datasets'], 'symbols')):
        rows = container[key]
        if not isinstance(rows, list) or any(not isinstance(r, dict) or not isinstance(r.get('id'), str) for r in rows):
            raise ValueError(f'{key} 不是預期的 symbol 物件陣列')
        container[key] = [r for r in rows if r['id'].casefold() in wanted]
    verify(original, result, symbols)
    return result


def verify(original, filtered, symbols):
    """獨立驗證：兩陣列只有刪除非指定商品，其餘完整深度比較。"""
    wanted = {s.casefold() for s in symbols}
    for label, source, actual in (
        ('availableSymbols', original['availableSymbols'], filtered['availableSymbols']),
        ('datasets.symbols', original['datasets']['symbols'], filtered['datasets']['symbols']),
    ):
        expected = [row for row in source if row['id'].casefold() in wanted]
        if canonical(actual) != canonical(expected):
            raise ValueError(f'{label} 的內容或順序被修改')
    restored = deepcopy(filtered)
    restored['availableSymbols'] = original['availableSymbols']
    restored['datasets']['symbols'] = original['datasets']['symbols']
    if canonical(restored) != canonical(original):
        raise ValueError('指定陣列以外的 JSON 內容有變動')


def download_one(exchange, symbols, output_dir, timeout=60):
    url = f'https://api.tardis.dev/v1/exchanges/{exchange}'
    request = Request(url, headers={'Accept': 'application/json', 'User-Agent': 'market-data-metadata/1.0'})
    with urlopen(request, timeout=timeout) as response:
        original = parse_json(response.read())
    if original.get('id') != exchange:
        raise ValueError(f'回應 exchange id 不符：{exchange}')
    filtered = filter_symbols(original, symbols)
    # 大小寫僅用於比對；不改寫原始 id（例如 Binance availableSymbols 為小寫）。
    missing = {}
    for label, rows in (('availableSymbols', filtered['availableSymbols']),
                        ('datasets.symbols', filtered['datasets']['symbols'])):
        present = {row['id'].casefold() for row in rows}
        absent = [s for s in symbols if s.casefold() not in present]
        if absent:
            missing[label] = absent
    destination = Path(output_dir) / f'{exchange}.json'
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=destination.parent,
                                         prefix='.metadata-', suffix='.tmp', delete=False) as f:
            temporary = Path(f.name)
            json.dump(filtered, f, ensure_ascii=False, indent=2, allow_nan=False)
            f.write('\n')
        # 驗證实际寫入的 JSON，而不僅是記憶體中的物件。
        verify(original, parse_json(temporary.read_bytes()), symbols)
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return destination, missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT_DIR)
    parser.add_argument('--workers', type=int, choices=range(1, 9), default=4)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    failed, missing_exchanges = [], []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        jobs = {pool.submit(download_one, ex, cfg['symbols'], args.output): ex
                for ex, cfg in EXCHANGE_CONFIG.items()}
        for future in as_completed(jobs):
            exchange = jobs[future]
            try:
                destination, missing = future.result()
                logging.info('%s 已保存並通過內容一致性驗證：%s', exchange, destination)
                if missing:
                    missing_exchanges.append(exchange)
                    logging.warning('%s 指定商品未出現在來源（未捏造或補值）：%s', exchange, missing)
            except Exception as exc:
                failed.append(exchange)
                logging.error('%s 失敗，既有檔案未替換：%s', exchange, exc)
    if failed or missing_exchanges:
        logging.error('下載失敗=%s；來源缺少指定商品=%s', failed, missing_exchanges)
        return 1
    logging.info('全部 %s 個 exchange 下載及驗證完成', len(EXCHANGE_CONFIG))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
