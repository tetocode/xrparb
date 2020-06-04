import json
import threading
from typing import Hashable, Tuple, Any

import coinlib
import coinlib.utils.config

from coinarb import utils
from coinarb.agents import agent
from coinarb.fundmanager import InsufficientFund
from coinarb.utils import gen_number


class Agent(agent.Agent):
    def __init__(self, *args, **kwargs):
        super().__init__('quoinex', *args, **kwargs)

        self.user_id = coinlib.utils.config.load()['quoinex']['user_id']
        self.client = coinlib.quoinex.StreamClient()
        self.order_book_update = threading.Condition()

        self.execution_callbacks = set()
        self.cancel_for_preparation()

    def cancel_for_preparation(self):
        order_book = self.client.get_order_book('XRP_JPY')
        price = (order_book['asks'][0][0] + order_book['bids'][0][0]) / 2
        self.cancel_matched_orders('XRP_JPY', lambda v: (price - 15) <= v['price'] <= (price + 15))

    def run(self):
        with coinlib.quoinex.StreamClient() as client:
            subscriptions = [
                ('execution', ('XRP_JPY', self.user_id)),
                ('execution', ('QASH_JPY', self.user_id)),
                ('execution', ('QASH_USD', self.user_id)),
            ]
            client.subscribe(*subscriptions, on_data=self.on_execution)
            threading.Thread(target=self.run_xrp_jpy_taker,
                             args=(self.config['xrp_jpy_taker'],),
                             daemon=True,
                             name=self.run_xrp_jpy_taker.__name__).start()
            threading.Thread(target=self.run_xrp_jpy_trap,
                             args=(self.config['xrp_jpy_trap'],),
                             daemon=True,
                             name=self.run_xrp_jpy_trap.__name__).start()
            threading.Thread(target=self.run_xrp_jpy_maker,
                             args=(self.config['xrp_jpy_maker'],),
                             daemon=True,
                             name=self.run_xrp_jpy_maker.__name__).start()
            super().run()

    def main(self):
        pass

    def on_data(self, exchange: str, key: Tuple[str, Hashable], on_data: Any):
        super().on_data(exchange, key, on_data)
        if key[0] == 'order_book':
            with self.order_book_update:
                self.order_book_update.notify_all()

    def on_execution(self, key: Tuple[str, Hashable], data: dict):
        self.logger.info('execution key={} data={}'.format(key, data))
        for callback in self.execution_callbacks:
            try:
                callback(key, data)
            except Exception as e:
                self.exception(e)

    def run_xrp_jpy_taker(self, taker_config: dict):
        self.wait_activation(timeout=10)
        start, stop, step = taker_config['diff_range']
        for diff in gen_number(start, stop, step, init=taker_config['diff_init'], loop=True):
            try:
                if not self.is_active():
                    break

                with self.order_book_update:
                    if not self.order_book_update.wait(timeout=1):
                        continue
                be_background = threading.Event()
                threading.Thread(target=self.try_execute_xrp_jpy_taker,
                                 name=self.try_execute_xrp_jpy_taker.__name__,
                                 args=(taker_config, diff, be_background)).start()
                be_background.wait(60)
            except InsufficientFund as e:
                self.logger.debug(e)
            except Exception as e:
                self.logger.exception(e)
                self.sleep(10)

    def try_execute_xrp_jpy_taker(self, taker_config: dict, diff_signal: float,
                                  be_background: threading.Event):
        try:
            diff_orig = diff_signal
            diff_signal = min([taker_config['diff_max'], diff_signal])
            diff_signal = max([taker_config['diff_min'], diff_signal])
            snapshot = self.get_order_books_snapshot()
            instrument = 'XRP_JPY'
            if not {('quoinex', instrument), ('bitbankcc', instrument)}.issubset(snapshot):
                return

            qty_min, qty_max = taker_config['qty_range']
            if taker_config['execute_n'] <= 0:
                return

            def try_arb(sell_exchange, buy_exchange, my_side):
                sell_order_book = snapshot[(sell_exchange, instrument)]
                buy_order_book = snapshot[(buy_exchange, instrument)]

                result_signal = utils.calculate_diff(sell_order_book, buy_order_book, diff_signal)
                if not result_signal or result_signal['diff'] < diff_signal:
                    return
                self.logger.info('signal={}'.format(json.dumps(result_signal, sort_keys=True)))
                diff_execute = diff_signal + taker_config['diff_execute_delta']
                result = utils.calculate_diff(sell_order_book, buy_order_book, diff_execute)
                if not result or result['diff'] < diff_execute:
                    return
                qty = result['qty']
                if qty < qty_min:
                    return
                taker_config['execute_n'] -= 1
                qty = min([qty, qty_max])
                assert {sell_exchange, buy_exchange}.issubset(set(self.agents)), set(self.agents)
                self.logger.info('execute={}'.format(json.dumps(result, sort_keys=True)))

                exchange_map = dict(SELL=sell_exchange, BUY=buy_exchange)
                currency_map = dict(SELL='XRP', BUY='JPY')
                reverse_side_map = dict(SELL='BUY', BUY='SELL')
                other_side = reverse_side_map[my_side]
                my_agent = self.agents[exchange_map[my_side]]
                other_agent = self.agents[exchange_map[other_side]]
                qty_map = {
                    'XRP': qty,
                    'JPY': qty,
                }
                my_qty = qty_map[currency_map[my_side]]
                other_qty = qty_map[currency_map[other_side]]
                my_price = result['{}_price'.format(my_side.lower())]
                other_price = result['{}_price'.format(other_side.lower())]
                my_fund = self.prepare_spot_fund(instrument,
                                                 order_type='limit', side=my_side,
                                                 price=my_price, qty=my_qty)
                other_fund = other_agent.prepare_spot_fund(instrument,
                                                           order_type='market', side=other_side,
                                                           price=other_price, qty=other_qty)
                with my_fund, other_fund:
                    order = my_fund.commit()
                    order = my_agent.cancel_order_wait(order, timeout=60)
                    if order['qty_executed'] <= 0:
                        self.logger.warning(
                            'order not filled order={}'.format(json.dumps(order, sort_keys=True)))
                        return
                    my_fund.consume()

                    be_background.set()
                    other_fund.commit(qty=order['qty_executed'])
                    other_fund.consume()
                return

            try_arb(sell_exchange='quoinex', buy_exchange='bitbankcc', my_side='SELL')
            try_arb(sell_exchange='bitbankcc', buy_exchange='quoinex', my_side='BUY')
        finally:
            be_background.set()

    def run_xrp_jpy_trap(self, trap_config: dict):
        """
        'xrp_jpy_trap': {
            'qty': 1.999999,
            'order_params': {
                'disc_quantity': 1,
            },
            'trap_delta_range': (1.0, 2.0, 0.1),
            'cancel_delta_range_out': (0.5, 1.0),
            'upper': True,
            'lower': True,
            'qty_reference': 10000,
            'update_wait': 60,
            'reentry_wait': 300,
        }
        """
        start, stop, step = trap_config['trap_delta_range']
        delta_list = []
        if trap_config['upper']:
            delta_list += [delta for delta in gen_number(start, stop, step)]
        if trap_config['lower']:
            delta_list += [-delta for delta in gen_number(start, stop, step)]
        for delta in delta_list:
            threading.Thread(target=self.run_xrp_jpy_trap_task,
                             args=(trap_config, delta),
                             name='xrp_jpy_trap_{}'.format(delta),
                             daemon=True).start()

    def run_xrp_jpy_trap_task(self, trap_config: dict, delta: float):
        instrument = 'XRP_JPY'
        cancel_min, cancel_max = trap_config['cancel_range_out']
        cancel_min, cancel_max = abs(delta) + cancel_min, abs(delta) + cancel_max
        if delta > 0:
            my_side = 'SELL'
            other_side = 'BUY'
            other_order_book_side = 'asks'
        else:
            my_side = 'BUY'
            other_side = 'SELL'
            other_order_book_side = 'bids'
        other_agent = self.agents['bitbankcc']

        def execute_trap():
            def get_order_book():
                with self.order_book_update:
                    if not self.order_book_update.wait(timeout=1):
                        return
                _snapshot = self.get_order_books_snapshot()
                return _snapshot.get(('bitbankcc', 'XRP_JPY'))

            def get_price() -> float:
                other_order_book = get_order_book()
                if not other_order_book:
                    return .0
                qty_sum = 0
                qty_ref = trap_config['qty_ref']
                _other_price = .0
                for v in other_order_book[other_order_book_side]:
                    qty_sum += v[1]
                    if qty_sum >= qty_ref:
                        _other_price = v[0]
                        break
                return _other_price

            other_price = get_price()
            if not other_price:
                return
            my_price = other_price + delta
            my_fund = self.prepare_spot_fund(instrument, 'limit', side=my_side, price=my_price,
                                             qty=trap_config['qty'])
            other_fund = other_agent.prepare_spot_fund(instrument, 'market', side=other_side,
                                                       price=other_price, qty=trap_config['qty'])
            with my_fund, other_fund:
                order = my_fund.commit(params=trap_config['order_params'])
                while self.is_active():
                    try:
                        price = get_price()
                        if price:
                            price_diff = abs(price - my_price)
                            if price_diff < cancel_min or cancel_max < price_diff:
                                self.logger.info('my_price={} price={} {} < {} or {} < {}'.format(
                                    my_price, price, price_diff, cancel_min, cancel_max, price_diff
                                ))
                                order = self.cancel_order_wait(order, timeout=60)

                        order = self.get_updated_order(order)
                        if order['state'] == 'ACTIVE' and order['qty_executed'] > 0:
                            order = self.cancel_order_wait(order, timeout=60)
                        if order['state'] != 'ACTIVE':
                            if order['qty_executed'] > 0:
                                my_fund.consume()
                                other_fund.commit(qty=order['qty_executed'])
                                other_fund.consume()
                            return
                    except Exception as _e:
                        self.logger.exception(_e)
                    self.sleep(trap_config['update_wait'])

        self.logger.info('xrp_jpy_trap start')
        self.wait_activation(timeout=10)
        while self.is_active():
            try:
                execute_trap()
            except InsufficientFund as e:
                self.logger.debug(e)
            except Exception as e:
                self.logger.exception(e)
            self.sleep(trap_config['update_wait'])
        self.logger.info('xrp_jpy_trap finish')

    def run_xrp_jpy_maker(self, maker_config: list):
        return
        for maker_config in maker_config:
            pass
