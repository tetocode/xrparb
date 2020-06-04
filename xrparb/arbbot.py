import contextlib
import functools
import itertools
import json
import logging
import threading
import time
from collections import OrderedDict
from typing import List

import coinlib.restapi.bitfinex2
from coinlib import bitfinex2, bitbankcc
from coinlib.coinlib.bitfinex2 import Flag
from coinlib.coinlib.errors import CoinLibError
from coinlib.utils import config
from coinlib.utils.mixins import LoggerMixin, ThreadMixin
from requests.structures import CaseInsensitiveDict

from xrparb.utils import get_depth
from .arbconfig import CONFIG
from .credentialpool import CredentialPool
from .dataprovider import DataProvider

logger = logging.getLogger(__name__)


class Shifter:
    def __init__(self, _config: dict):
        self.config = _config
        self.diff_max = self.diff_max_init = _config['diff_max_init']
        self.diff_min = self.diff_min_init = _config['diff_min_init']
        self.execution_shift = 0

    def get_sell_shift(self) -> float:
        return self.diff_max * 0.9 + self.execution_shift

    def get_buy_shift(self) -> float:
        return self.diff_min * 0.9 + self.execution_shift

    def update(self):
        shift = self.config['shift_step']
        if self.execution_shift > 0:
            self.execution_shift -= shift
        elif self.execution_shift < 0:
            self.execution_shift += shift
        self.execution_shift = round(self.execution_shift, 8)
        if abs(self.execution_shift) <= shift:
            self.execution_shift = 0

        self.diff_max = max([self.diff_max_init, self.diff_max - shift])
        self.diff_min = min([self.diff_min_init, self.diff_min + shift])
        logger.info('### shift min={} max={} exe={} sell={} buy={}'.format(
            self.diff_min, self.diff_max, self.execution_shift,
            self.get_sell_shift(), self.get_buy_shift(),
        ))

    def diff(self, diff: float):
        self.diff_max = max([self.diff_max, diff])
        self.diff_min = min([self.diff_min, diff])

    def execution(self, side: str, qty: float):
        shift = qty * self.config['execution_shift_rate']
        if side.lower() == 'buy':
            self.execution_shift -= shift
        else:
            self.execution_shift += shift


class Bot(LoggerMixin, ThreadMixin):
    def __init__(self, data_provider: DataProvider, **__):
        self._logger = self._make_logger()
        self._thread_data = self._make_thread_data()
        self._data_provider = data_provider
        self._bitbankcc_credential_pool = CredentialPool(config.load()['bitbankcc']['credentials'])
        self._bitbankcc_lock = threading.RLock()
        self._bitfinex2_credential_pool = CredentialPool(config.load()['bitfinex2']['credentials'])
        self._bitfinex1_credential_pool = CredentialPool(config.load()['bitfinex']['xrparb'])
        self._bitfinex2_client = None  # type: bitfinex2.StreamClient
        self._bitfinex2_lock = threading.RLock()
        self.config = CONFIG
        self.shifter = Shifter(self.config['shifter'])

    @contextlib.contextmanager
    def get_bitbankcc(self) -> bitbankcc.StreamClient:
        with self._bitbankcc_lock:
            with self._bitbankcc_credential_pool.get() as credential:
                yield bitbankcc.StreamClient(credential['api_key'], credential['api_secret'])

    @contextlib.contextmanager
    def get_bitfinex2(self) -> bitfinex2.Account:
        with self._bitfinex2_lock:
            self._bitfinex2_client.wait_authentication(timeout=10)
            yield self._bitfinex2_client.account

    def now(self) -> float:
        _ = self
        return time.time()

    def sleep(self, seconds: float):
        _ = self
        time.sleep(seconds)

    def round_price(self, exchange: str, instrument: str, price: float) -> float:
        precisions = self.config[exchange]['precisions']
        assert instrument in precisions, (exchange, instrument)
        precision = precisions[instrument]['price']
        return int(price * 10 ** precision) / (10 ** precision)

    def inc_dec_price(self, exchange: str, instrument: str, price: float, inc_dec: int) -> float:
        precisions = self.config[exchange]['precisions']
        assert instrument in precisions, (exchange, instrument)
        precision = precisions[instrument]['price']
        price += inc_dec / (10 ** precision)
        return self.round_price(exchange, instrument, price)

    def round_qty(self, exchange: str, instrument: str, qty: float) -> float:
        precisions = self.config[exchange]['precisions']
        assert instrument in precisions, (exchange, instrument)
        precision = precisions[instrument]['qty']
        return int(qty * 10 ** precision) / (10 ** precision)

    def inc_dec_qty(self, exchange: str, instrument: str, qty: float, inc_dec: int) -> float:
        precisions = self.config[exchange]['precisions']
        assert instrument in precisions, (exchange, instrument)
        precision = precisions[instrument]['qty']
        qty += inc_dec / (10 ** precision)
        return self.round_qty(exchange, instrument, qty)

    def commit_fund(self, order_fund, *, qty: float = None, params: dict = None) -> dict:
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

    def get_updated_order_books(self):
        return self._data_provider.get_updated_order_books()

    def run(self):
        with self._bitfinex2_credential_pool.get() as credential:
            self._bitfinex2_client = bitfinex2.StreamClient(credential['api_key'], credential['api_secret'])
        with self._bitfinex2_client:
            self._bitfinex2_client.subscribe(account=True)
            threads = []
            assert self._bitfinex2_client.wait_authentication(timeout=10)

            # with self.get_bitfinex2() as account:
            #     account = account  # type: bitfinex2.Account
            #     account.submit_order_op(account.cancel_group_order_op(1), async=True)
            #     account.submit_order_op(account.cancel_group_order_op(2), async=True)

            threads.append(threading.Thread(target=self.run_taker, daemon=True))
            threads.append(threading.Thread(target=self.run_update_order, daemon=True))
            threads.append(threading.Thread(target=self.run_maker_sell, daemon=True))
            threads.append(threading.Thread(target=self.run_maker_buy, daemon=True))
            for t in threads:
                t.start()
            while self.is_active():
                self.shifter.update()
                self.sleep(1)
            for t in threads:
                t.join(timeout=1)
            # with self.get_bitfinex2() as account:
            #     account = account  # type: bitfinex2.Account
            #     account.submit_order_op(account.cancel_group_order_op(1), async=True)
            #     account.submit_order_op(account.cancel_group_order_op(2), async=True)

    def run_taker(self):
        reverse_map = CaseInsensitiveDict(BUY='SELL', SELL='BUY')

        # db_client = pymongo.MongoClient()
        # collection = db_client['xrparb']['trades']
        # collection.create_index([('id', -1)], unique=True)

        def get_trades():
            with self._bitfinex2_credential_pool.get() as credential:
                api = coinlib.restapi.bitfinex2.RestApi(credential['api_key'], credential['api_secret'])
                res = api.private_post('/auth/r/trades/tXRPUSD/hist', limit=500)
            results = []
            keys = [
                'id', 'pair', 'mts_create', 'order_id', 'exec_amount',
                'exec_price', 'order_type', 'order_price',
                'maker', 'fee', 'fee_currency',
            ]
            for x in res:
                d = dict(zip(keys, x))
                exec_amount = d['exec_amount']
                side = 'BUY' if exec_amount > 0 else 'SELL'
                v = {
                    'timestamp': d['mts_create'] / 1000,
                    'id': d['id'],
                    'instrument': 'XRP_USD',
                    'order_type': d['order_type'],
                    'side': side,
                    'qty_executed': abs(exec_amount),
                    '_data': d,
                    '_extra': x[len(keys):],
                    'qty_cross': 0,
                }
                results.append(v)
            return results

        past_trades = OrderedDict()
        for i in itertools.count():
            if not self.is_active():
                break
            self.sleep(1)
            try:
                if i % 30 == 0:
                    for x in get_trades():
                        if x['id'] not in past_trades:
                            if i == 0:
                                x['qty_cross'] = x['qty_executed']
                            else:
                                self.logger.info('new trade={}'.format(json.dumps(x, sort_keys=True)))
                            past_trades.setdefault(x['id'], x)
                with self.get_bitfinex2() as account:
                    trades = account.trades.get('XRP_USD')
                    if not trades:
                        continue
                    while len(trades):
                        trade = trades.popleft()
                        if trade['_type'] == 'tu':
                            trade['id'] = trade['execution_id']
                            trade['qty_cross'] = 0
                            if trade['id'] not in past_trades:
                                self.logger.info('new trade={}'.format(json.dumps(trade, sort_keys=True)))
                                past_trades.setdefault(trade['id'], trade)
                for trade_id in itertools.islice(reversed(past_trades), 0, 500):
                    trade = past_trades[trade_id]
                    if not (trade['instrument'] == 'XRP_USD' and trade['order_type'] == 'LIMIT'):
                        continue
                    while trade['qty_cross'] < trade['qty_executed']:
                        qty_diff = trade['qty_executed'] - trade['qty_cross']
                        if qty_diff > 1000:
                            qty_diff = 1000
                        with self.get_bitbankcc() as client:
                            qty = self.round_qty('bitbankcc', 'XRP_JPY', qty_diff)
                            side = reverse_map[trade['side']]
                            self.logger.info('execute taker order {} {} {}'.format('XRP_JPY', side, qty))
                            if qty:
                                client.create_market_order('XRP_JPY',
                                                           side=side,
                                                           qty=qty)
                            else:
                                self.logger.info('skip execute taker order {} {} {}'.format('XRP_JPY', side, qty))
                        trade['qty_cross'] += qty_diff
                        self.shifter.execution(trade['side'], qty_diff)
                        self.sleep(1)
            except Exception as e:
                self.logger.exception(e)
                self.sleep(2)

    order_update_map_lock = threading.RLock()
    order_update_map = OrderedDict()

    def run_update_order(self):
        round_xrp_usd = functools.partial(self.round_price, 'bitfinex2', 'XRP_USD')
        while self.is_active():
            self.sleep(self.config['order_update_interval'])
            try:
                with self.order_update_map_lock:
                    if not len(self.order_update_map):
                        continue
                    order_id, (gid, price) = self.order_update_map.popitem(last=False)
                with self.get_bitfinex2() as account:
                    price = round_xrp_usd(price)
                    op = account.update_order_op(order_id, price=price,
                                                 # gid=gid,
                                                 # flags=[Flag.POST_ONLY],
                                                 )
                    account.submit_order_op(op)
            except Exception as e:
                self.logger.exception(e)

    unhidden_lock = threading.RLock()
    unhidden_order_ids = set()

    def unhidden_order_bg(self, order_id: int, price: float, inc_dec: int):
        inc_dec_xrp_usd = functools.partial(self.inc_dec_price, 'bitfinex2', 'XRP_USD')

        def unhidden():
            nonlocal price
            for i in range(100):
                if not self.is_active():
                    break
                try:
                    self.logger.info('try unhidden {} {} {}'.format(order_id, price, inc_dec))
                    with self.get_bitfinex2() as account:
                        price = inc_dec_xrp_usd(price, inc_dec)
                        op1 = account.update_order_op(order_id, price=price, flags=[Flag.POST_ONLY, Flag.HIDDEN])
                        # account.submit_order_op(op2, async=True)
                        account.submit_order_op(op1)
                    time.sleep(1)
                    order = self._bitfinex2_client.get_order('', order_id)
                    if not order['is_hidden']:
                        if bool(account.orders[order_id]['_data']['flags'] & 64):
                            self.unhidden_order_ids.add(order_id)
                            self.logger.info('end unhidden {} {} {}'.format(order_id, price, inc_dec))
                            break
                except Exception as e:
                    self.logger.exception(e)
                finally:
                    self.sleep(1)
            with self.get_bitfinex2() as account:
                price = inc_dec_xrp_usd(price, inc_dec)
                op1 = account.update_order_op(order_id, price=price, flags=[Flag.POST_ONLY])
                account.submit_order_op(op1)
                self.unhidden_order_ids.add(order_id)

        def unhidden_wrap():
            with self.unhidden_lock:
                unhidden()
                time.sleep(10)

        threading.Thread(target=unhidden_wrap, daemon=True).start()

    def run_maker_sell(self):
        config = self.config['sell']
        if not config['enable']:
            return
        position_min = - config['position_max']
        order_qty_min = config['order_qty']
        max_n = config['unit_n']
        jpy_diff_min = config['jpy_diff_min']
        jpy_diff = jpy_diff_min
        diff_step = config['diff_step']
        init = False

        round_xrp_usd = functools.partial(self.round_price, 'bitfinex2', 'XRP_USD')
        while self.is_active():
            self.sleep(config['interval'])
            jpy_diff -= config['jpy_diff_decrement']
            jpy_diff = max([jpy_diff_min, jpy_diff])
            try:
                order_books = self.get_updated_order_books()
                target = order_books.get(('bitbankcc', 'XRP_JPY'))
                trap = order_books.get(('bitfinex2', 'XRP_USD'))
                if not target or not trap:
                    continue
                with self.get_bitfinex2() as account:
                    position = account.margin_positions.get('XRP_USD')
                account = account  # type: bitfinex2.Account
                # active orders only
                order_ids = []
                for order_id, order in sorted(account.orders.items()):
                    if order['_data']['gid'] == 1 and order['state'] == 'ACTIVE':
                        order_ids.append(order_id)
                order_qty = order_qty_min
                if not position:
                    position_qty = 0
                else:
                    position_qty = position['_data']['amount']
                order_n = int((position_qty - position_min) / order_qty)
                if order_n < 1:
                    continue
                order_n = min([order_n, max_n])
                usd_jpy = self._data_provider.fx_rates['USD_JPY']['mid']
                depth = get_depth(target['asks'], config['depth'])
                if not depth:
                    continue
                jpy, qty, _ = depth
                depth_usd = get_depth(trap['bids'], config['depth'])
                _diff = depth_usd[0] - jpy
                if _diff > 0:
                    self.shifter.diff(_diff)
                maker_price = (jpy + self.shifter.get_sell_shift()) / usd_jpy
                maker_best_ask = trap['asks'][0][2]
                maker_price = max([maker_price, maker_best_ask])
                for i, order_id in enumerate(order_ids, 1):
                    maker_price = round_xrp_usd(maker_price)
                    order = account.orders[order_id]
                    price_diff = order['price'] - maker_price
                    # if order_id not in self.unhidden_order_ids:
                    #     continue
                    if price_diff > diff_step or price_diff < -diff_step:
                        with self.order_update_map_lock:
                            self.order_update_map[order_id] = (1, maker_price)
                        self.logger.info(
                            '* order_id={} side={} price {:.5} -> {:.5} ({:+.5})'.format(order_id, order['side'],
                                                                                         float(order['price']),
                                                                                         float(maker_price),
                                                                                         float(-price_diff)))
                        self.logger.info(
                            '* side={} jpy_diff_base={:.8} jpy={:.3} maker={:.3}({:.5}) jpy_diff={:+.3}'.format(
                                order['side'], float(jpy_diff), float(jpy), float(maker_price * usd_jpy),
                                float(maker_price),
                                float(maker_price * usd_jpy - jpy)
                            ))
                    maker_price += diff_step * 1
                if order_n > len(order_ids):
                    for _ in range(order_n - len(order_ids)):
                        new_price = round_xrp_usd(maker_price * 1.2)
                        op = account.new_order_op('XRP_USD', 'LIMIT', 'SELL',
                                                  price=new_price, qty=order_qty,
                                                  gid=1)
                        with self.get_bitfinex2() as account:
                            order_id = account.submit_order_op(op)
                            new_price = round_xrp_usd(maker_price * 1.19)
                            op = account.update_order_op(order_id, price=new_price, flags=[Flag.POST_ONLY, Flag.HIDDEN])
                            account.submit_order_op(op)
                        maker_price += diff_step
                        if init:
                            jpy_diff += config['jpy_diff_reentry_adjustment']
                            self.logger.info('jpy_diff={}'.format(jpy_diff))
                        time.sleep(1)
                    init = True

            except Exception as e:
                self.logger.exception(e)

    def run_maker_buy(self):
        config = self.config['buy']
        if not config['enable']:
            return
        position_max = config['position_max']
        order_qty_min = config['order_qty']
        max_n = config['unit_n']
        jpy_diff_min = config['jpy_diff_min']
        jpy_diff = jpy_diff_min
        diff_step = config['diff_step']
        init = False

        round_xrp_usd = functools.partial(self.round_price, 'bitfinex2', 'XRP_USD')
        while self.is_active():
            self.sleep(config['interval'])
            jpy_diff -= config['jpy_diff_decrement']
            jpy_diff = max([jpy_diff_min, jpy_diff])
            try:
                order_books = self.get_updated_order_books()
                target = order_books.get(('bitbankcc', 'XRP_JPY'))
                trap = order_books.get(('bitfinex2', 'XRP_USD'))
                if not target or not trap:
                    continue
                with self.get_bitfinex2() as account:
                    position = account.margin_positions.get('XRP_USD')
                account = account  # type: bitfinex2.Account
                # active orders only
                order_ids = []
                for order_id, order in sorted(account.orders.items()):
                    if order['_data']['gid'] == 2 and order['state'] == 'ACTIVE':
                        order_ids.append(order_id)
                order_qty = order_qty_min
                if not position:
                    position_qty = 0
                else:
                    position_qty = position['_data']['amount']
                order_n = int((position_max - position_qty) / order_qty)
                if order_n < 1:
                    continue
                order_n = min([order_n, max_n])

                usd_jpy = self._data_provider.fx_rates['USD_JPY']['mid']
                depth = get_depth(target['bids'], config['depth'])
                if not depth:
                    continue
                jpy, qty, _ = depth
                depth_usd = get_depth(trap['asks'], config['depth'])
                _diff = depth_usd[0] - jpy
                if _diff < 0:
                    self.shifter.diff(_diff)
                maker_price = (jpy + self.shifter.get_buy_shift()) / usd_jpy
                maker_best_bid = trap['bids'][0][2]
                maker_price = min([maker_price, maker_best_bid])
                for i, order_id in enumerate(order_ids, 1):
                    maker_price = round_xrp_usd(maker_price)
                    order = account.orders[order_id]
                    price_diff = maker_price - order['price']
                    # if order_id not in self.unhidden_order_ids:
                    #     continue
                    if price_diff > diff_step or price_diff < -diff_step:
                        with self.order_update_map_lock:
                            self.order_update_map[order_id] = (2, maker_price)
                        self.logger.info(
                            '* order_id={} side={} price {:.5} -> {:.5} ({:+.5})'.format(order_id, order['side'],
                                                                                         float(order['price']),
                                                                                         float(maker_price),
                                                                                         float(price_diff)))
                        self.logger.info(
                            '* side={} jpy_diff_base={:.8} jpy={:.3} maker={:.3}({:.5}) jpy_diff={:+.3}'.format(
                                order['side'], float(jpy_diff), float(jpy), float(maker_price * usd_jpy),
                                float(maker_price),
                                float(maker_price * usd_jpy - jpy)
                            ))
                    maker_price -= diff_step * 1
                if order_n > len(order_ids):
                    for _ in range(order_n - len(order_ids)):
                        new_price = round_xrp_usd(maker_price * 0.8)
                        op = account.new_order_op('XRP_USD', 'LIMIT', 'BUY',
                                                  price=new_price, qty=order_qty,
                                                  gid=2)
                        with self.get_bitfinex2() as account:
                            order_id = account.submit_order_op(op)
                            new_price = round_xrp_usd(maker_price * 0.81)
                            op = account.update_order_op(order_id, price=new_price, flags=[Flag.POST_ONLY, Flag.HIDDEN])
                            account.submit_order_op(op)
                        maker_price -= diff_step
                        if init:
                            jpy_diff += config['jpy_diff_reentry_adjustment']
                            self.logger.info('jpy_diff={}'.format(jpy_diff))
                        time.sleep(1)
                    init = True

            except Exception as e:
                self.logger.exception(e)
