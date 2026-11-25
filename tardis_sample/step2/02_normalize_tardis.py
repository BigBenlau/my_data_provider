#!/usr/bin/env python3
"""舊版 Binance 入口，既有呼叫轉交交易所專用模組。"""
from functools import partial
from importlib import import_module
from normalization import cli

_binance = import_module('02_normalize_tardis_binance')
BookError, Book, BookEngine = _binance.BookError, _binance.Book, _binance.BookEngine
Mapper, normalize = _binance.Mapper, _binance.normalize
main = partial(cli, Mapper)


if __name__ == '__main__':
    main()
