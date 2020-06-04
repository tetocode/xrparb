import contextlib
import threading
import time
from queue import Queue
from typing import Dict, Type, List

import coinlib
from coinlib.coinlib.errors import CoinLibError
from coinlib.utils import config
from coinlib.utils.mixins import LoggerMixin, ThreadMixin

from coinarb.arbconfig import CONFIG
from coinarb.credentialpool import CredentialPool
from coinarb.fundmanager import FundManager
from coinarb.spotfund import SpotFund


class Agent(LoggerMixin, ThreadMixin):
    BALANCE_UPDATE_INTERVAL = 60

    def __init__(self, exchange: str, debug: bool = False, **__):
        self.name = exchange
        self._logger = self._make_logger()
        self._thread_data = self._make_thread_data()
        self.is_debug = debug
        self.order_books = {}
        self.config = CONFIG[exchange]
        self._client_cls = getattr(coinlib, exchange).StreamClient  # type: Type[coinlib.StreamClient]
        self.agents = {}  # type: Dict[str, Agent]
        self.fund_manager = FundManager(exchange, self.config['funds'])
        self._credential_pool = CredentialPool(config.load()[self.name]['credentials'])
        self._task_q = Queue()
        self.is_balance_updated = threading.Event()

        if debug:
            self.logger.info('DEBUG MODE')

    @contextlib.contextmanager
    def get_client(self) -> coinlib.StreamClient:
        with self._credential_pool.get() as credential:
            yield self._client_cls(credential['api_key'], credential['api_secret'])

    def sleep(self, seconds: float):
        _ = self
        time.sleep(seconds)

    def start_interval_task(self, interval: float, func, *args, **kwargs):
        def run():
            while self.is_active():
                try:
                    func(*args, **kwargs)
                except Exception as e:
                    self.logger.exception(e)
                self.sleep(interval)

        threading.Thread(target=run, daemon=True).start()

    def start(self, *args, **kwargs):
        self.update_balances()
        super().start(*args, **kwargs)

    def run(self):
        self.start_interval_task(self.BALANCE_UPDATE_INTERVAL, self.update_balances)
        while self.is_active():
            self.sleep(0.2)

    def update_balances(self):
        with self.get_client() as client:
            balances = client.get_balances()
        self.fund_manager.update_balances(balances)
        self.is_balance_updated.set()

    def round_price(self, instrument: str, price: float) -> float:
        precisions = self.config['precisions']
        assert instrument in precisions, (self.name, instrument)
        precision = precisions[instrument]['price']
        return int(price * 10 ** precision) / (10 ** precision)

    def inc_dec_price(self, instrument: str, price: float, inc_dec: int) -> float:
        precisions = self.config['precisions']
        assert instrument in precisions, (self.name, instrument)
        precision = precisions[instrument]['price']
        price += inc_dec / (10 ** precision)
        return self.round_price(instrument, price)

    def round_qty(self, instrument: str, qty: float) -> float:
        precisions = self.config['precisions']
        assert instrument in precisions, (self.name, instrument)
        precision = precisions[instrument]['qty']
        return int(qty * 10 ** precision) / (10 ** precision)

    def inc_dec_qty(self, instrument: str, qty: float, inc_dec: int) -> float:
        precisions = self.config['precisions']
        assert instrument in precisions, (self.name, instrument)
        precision = precisions[instrument]['qty']
        qty += inc_dec / (10 ** precision)
        return self.round_qty(instrument, qty)

    def prepare_spot_fund(self, instrument: str, order_type: str, side: str, price: float, qty: float,
                          *, order_id: int = 0, condition: str = '', **__):
        return SpotFund(self.fund_manager, instrument=instrument, order_type=order_type,
                        side=side, price=price, qty=qty, order_id=order_id, condition=condition,
                        commit=self.commit_fund)

    def commit_fund(self, order_fund: SpotFund, *, qty: float = None, params: dict = None) -> dict:
        instrument = order_fund.instrument
        order_type = order_fund.order_type
        side = order_fund.order_side
        price = order_fund.order_price
        qty = qty or order_fund.order_qty
        fund = order_fund
        params = params or params

        price = self.inc_dec_price(instrument, price, 1 if side == 'BUY' else -1)
        qty = self.round_qty(instrument, qty)
        with self.get_client() as client:
            self.logger.info(
                'commit instrument={} order_type={} side={} price={} qty={} fund={}'.format(
                    instrument, order_type, side, price, qty, fund
                ))
            if self.is_debug:
                order = dict(price=price,
                             price_executed_average=price,
                             qty=qty,
                             qty_executed=qty,
                             qty_remained=0,
                             debug=True)
            else:
                order_type = order_type.lower()
                if order_type == 'market':
                    order = client.create_market_order(instrument, side=side, qty=qty, params=params)
                elif order_type == 'limit':
                    order = client.create_limit_order(instrument, side=side, price=price, qty=qty, params=params)
                else:
                    assert False, order_type
            self.logger.info(
                'executed price_executed={price_executed_average} qty_executed={qty_executed}'.format(**order))
        return order

    def get_active_orders(self, instrument: str) -> List[dict]:
        with self.get_client() as client:
            return client.get_active_orders(instrument)

    def get_updated_order(self, order: dict) -> dict:
        with self.get_client() as client:
            return client.get_order(**order)

    def cancel_order_wait(self, order: dict, timeout: float) -> dict:
        expired = time.time() + timeout
        while time.time() < expired:
            try:
                with self.get_client() as client:
                    order = client.get_order(**order)
                    self.logger.debug('order={}'.format(order))
                    if order['state'] == 'ACTIVE':
                        self.logger.info('try cancel_order order={}'.format(order))
                        try:
                            client.cancel_order(**order)
                        except CoinLibError:
                            pass
                    else:
                        return order
            except Exception as e:
                self.logger.exception(str(e))
                self.sleep(5)
            self.sleep(0.5)
        raise Exception('order not completed order={}'.format(order))

    def cancel_matched_orders(self, instrument: str, match_func):
        with self.get_client() as client:
            for order in client.get_active_orders(instrument):
                if match_func(order):
                    self.logger.info('cancel matched order={}'.format(order))
                    client.cancel_order(**order)
