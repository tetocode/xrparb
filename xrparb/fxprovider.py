import time
from typing import List

import dateutil.parser
import dateutil.parser
import oandapy
from coinlib.utils import config
from coinlib.utils.mixins import LoggerMixin, ThreadMixin


class FxProvider(LoggerMixin, ThreadMixin):
    def __init__(self, instruments: List[str]):
        self.instruments = frozenset(instruments)
        self._callbacks = set()
        self._logger = self._make_logger()
        self._thread_data = self._make_thread_data()

    def register_callback(self, on_data):
        self._callbacks.add(on_data)

    def dispatch_price(self, price: dict):
        """
        {
            "prices" : [
                {
                    "instrument" : "EUR_USD",
                    "time" : "2013-09-16T18:59:03.687308Z",
                    "bid" : 1.33319,
                    "ask" : 1.33326
                }
            ]
        }
        """
        price['mid'] = (price['ask'] + price['bid']) / 2
        price['timestamp'] = dateutil.parser.parse(price['time']).timestamp()
        self.dispatch_data(('tick', price['instrument']), price)

    def dispatch_data(self, key, data):
        for callback in self._callbacks:
            try:
                callback('oanda', key, data)
            except Exception as e:
                self.logger.exception(e)

    def run(self):
        credential = config.load()['oanda']['live']
        oanda = oandapy.API(environment='live', access_token=credential['access_token'])
        while self.is_active():
            try:
                res = oanda.get_prices(instruments=','.join(self.instruments))
                for price in res['prices']:
                    self.dispatch_price(price)
            except Exception as e:
                self.logger.exception(e)
            for _ in range(100):
                time.sleep(0.1)
                if not self.is_active():
                    break
