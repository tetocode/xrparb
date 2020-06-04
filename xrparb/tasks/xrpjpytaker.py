import json
import threading
from concurrent.futures import ThreadPoolExecutor

from coinarb import utils
from coinarb.fundmanager import InsufficientFund
from coinarb.utils import gen_number
from . import task


class Task(task.Task):
    thread_pool = ThreadPoolExecutor()
    bitbankcc_lock = threading.RLock()

    def run(self):
        taker_config = self.config
        start, stop, step = taker_config['diff_range']
        for diff in gen_number(start, stop, step, init=taker_config['diff_init'], loop=True):
            try:
                if not self.is_active():
                    break

                be_background = threading.Event()
                self.thread_pool.submit(self.try_execute_xrp_jpy_taker,
                                        taker_config, diff, be_background)
                be_background.wait(60)
            except InsufficientFund as e:
                self.logger.debug(e)
            except Exception as e:
                self.logger.exception(e)
                self.sleep(10)

    def try_execute_xrp_jpy_taker(self, taker_config: dict, diff_signal: float,
                                  be_background: threading.Event):
        try:
            diff_signal = min([taker_config['diff_max'], diff_signal])
            diff_signal = max([taker_config['diff_min'], diff_signal])
            snapshot = self.get_updated_order_books()
            instrument = 'XRP_JPY'
            my_exchange = 'quoinex'
            other_exchange = 'bitbankcc'
            if not {(my_exchange, instrument), (other_exchange, instrument)}.issubset(snapshot):
                return
            qty_min, qty_max = taker_config['qty_range']
            if taker_config['execute_n'] <= 0:
                return

            def try_arb(my_side: str, other_side: str):
                if my_side == 'SELL':
                    sell_order_book = snapshot[(my_exchange, instrument)]
                    buy_order_book = snapshot[(other_exchange, instrument)]
                else:
                    sell_order_book = snapshot[(other_exchange, instrument)]
                    buy_order_book = snapshot[(my_exchange, instrument)]
                my_agent = self.agents[my_exchange]
                other_agent = self.agents[other_exchange]

                result_signal = utils.calculate_diff(sell_order_book, buy_order_book, diff_signal)
                if not result_signal or result_signal['diff'] < diff_signal:
                    return
                diff_execute = diff_signal + taker_config['diff_execute_delta']
                result = utils.calculate_diff(sell_order_book, buy_order_book, diff_execute)
                if not result or result['diff'] < diff_execute:
                    return
                qty = result['qty']
                if qty < qty_min:
                    return
                taker_config['execute_n'] -= 1
                qty = min([qty, qty_max])

                my_price = result['{}_price'.format(my_side.lower())]
                other_price = result['{}_price'.format(other_side.lower())]
                my_fund = my_agent.prepare_spot_fund(instrument,
                                                     order_type='limit', side=my_side,
                                                     price=my_price, qty=qty)
                other_fund = other_agent.prepare_spot_fund(instrument,
                                                           order_type='market', side=other_side,
                                                           price=other_price, qty=qty)
                with my_fund, other_fund:
                    self.logger.info('signal={}'.format(json.dumps(result_signal, sort_keys=True)))
                    self.logger.info('execute={}'.format(json.dumps(result, sort_keys=True)))

                    order = my_fund.commit()
                    order = my_agent.cancel_order_wait(order, timeout=60)
                    if order['qty_executed'] <= 0:
                        self.logger.warning(
                            'order not filled order={}'.format(json.dumps(order, sort_keys=True)))
                        return
                    my_fund.consume()

                    be_background.set()
                    with self.bitbankcc_lock:
                        other_fund.commit(qty=order['qty_executed'])
                        other_fund.consume()
                return

            try_arb(my_side='SELL', other_side='BUY')
            try_arb(my_side='BUY', other_side='SELL')
        except InsufficientFund as e:
            self.logger.info('insufficient fund {}'.format(e))
            self.logger.info('sleep 30.0')
            self.sleep(30)
        except Exception as e:
            self.logger.exception(e)
            raise
        finally:
            be_background.set()
