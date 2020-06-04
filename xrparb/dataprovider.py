import functools
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple, Any, Hashable, Dict

import coinlib
from coinlib.utils.mixins import LoggerMixin, ThreadMixin

from . import utils
from .fxprovider import FxProvider


class DataProvider(LoggerMixin, ThreadMixin):
    ORDER_BOOK_TTL = 2.5
    FX_TTL = 60

    def __init__(self, order_books: List[Tuple[str, str]] = None, *, fx_provider: FxProvider):
        self._logger = self._make_logger()
        self._thread_data = self._make_thread_data()
        order_book_subscriptions = defaultdict(list)
        for exchange, instrument in order_books:
            order_book_subscriptions[exchange].append(instrument)
        self._order_book_subscriptions = order_book_subscriptions
        self.clients = {
            exchange: getattr(coinlib, exchange).StreamClient()
            for exchange in self._order_book_subscriptions
        }
        self._order_books = {}
        self._order_book_update = threading.Condition()

        self.fx_rates = {}
        fx_provider.register_callback(self.on_fx_data)

    def start(self, *_, **__):
        self._subscribe_all()
        super().start()

    def stop(self):
        super().stop()
        for client in self.clients.values():
            client.close()

    def _subscribe_all(self):
        for exchange, instruments in self._order_book_subscriptions.items():
            client = self.clients[exchange]
            client.open()
            subscriptions = [('order_book', instrument) for instrument in instruments]
            on_data = functools.partial(self.on_order_book, exchange)
            client.subscribe(*subscriptions, on_data=on_data)

    def adjust_order_book(self, order_book: dict):
        converted_order_book = order_book.copy()
        instrument = order_book['instrument']
        try:
            if instrument.endswith('_JPY'):
                rate = 1
            elif instrument.endswith('_USD'):
                data = self.fx_rates['USD_JPY']
                #if data['timestamp'] < time.time() - self.FX_TTL:
                #    return None
                rate = data['mid']
            else:
                raise IndexError('not supported currency {}'.format(instrument))
        except IndexError:
            return None

        asks, bids = utils.adjust_asks_bids(order_book['asks'], order_book['bids'], rate)
        converted_order_book['asks'] = asks
        converted_order_book['bids'] = bids
        return converted_order_book

    def on_order_book(self, exchange: str, key: Tuple[str, Hashable], order_book: Any):
        order_book = self.adjust_order_book(order_book)
        if not order_book:
            return
        with self._order_book_update:
            self._order_books[(exchange, key[1])] = order_book
            self._order_book_update.notify_all()

    def get_updated_order_books(self, timeout: float = 60) -> Dict[Tuple[str, str], dict]:
        with self._order_book_update:
            if not self._order_book_update.wait(timeout=timeout):
                return {}
            snapshot = self._order_books.copy()
        now = time.time()
        return {k: v for k, v in snapshot.items() if abs(now - v['timestamp']) <= self.ORDER_BOOK_TTL}

    def on_fx_data(self, exchange: str, key: Tuple[str, Hashable], data: dict):
        _, _ = exchange, key
        self.fx_rates[data['instrument']] = data

    def sleep(self, seconds: float):
        _ = self
        time.sleep(seconds)

    def run(self):
        while self.is_active():
            self.sleep(0.5)
