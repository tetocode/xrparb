import math
from typing import Optional, Tuple, List


def adjust_asks_bids(asks: List[Tuple[float, float]], bids: List[Tuple[float, float]], rate: float):
    if not asks or not bids:
        return ([(price * rate, qty, price) for price, qty in asks],
                [(price * rate, qty, price) for price, qty in bids])

    asks_it = iter(asks)
    bids_it = iter(bids)
    ask = next(asks_it)
    bid = next(bids_it)
    while True:
        if ask[0] <= bid[0]:
            qty_diff = ask[1] - bid[1]
            if qty_diff > 0:
                ask = (ask[0], qty_diff)
                bid = next(bids_it)
            elif qty_diff < 0:
                bid = (bid[0], abs(qty_diff))
                ask = next(asks_it)
            else:
                ask = next(asks_it)
                bid = next(bids_it)
        else:
            break
    asks = [(ask[0] * rate, ask[1], ask[0])]
    asks += [(price * rate, qty, price) for price, qty in asks_it]
    bids = [(bid[0] * rate, bid[1], bid[0])]
    bids += [(price * rate, qty, price) for price, qty in bids_it]
    return asks, bids


def calculate_diff(order_book_for_sell: dict,
                   order_book_for_buy: dict,
                   diff_min: float,
                   sell_qty_adjustment: float = 0,
                   buy_qty_adjustment: float = 0) -> Optional[dict]:
    if 'bids' not in order_book_for_sell:
        raise Exception('{}'.format(order_book_for_sell))
    if 'asks' not in order_book_for_buy:
        raise Exception('{}'.format(order_book_for_buy))
    bids_for_sell = order_book_for_sell['bids']
    asks_for_buy = order_book_for_buy['asks']
    sell_jpy = math.nan
    sell_qty = math.nan
    sell_price = math.nan
    buy_jpy = math.nan
    buy_qty = math.nan
    buy_price = math.nan

    def check_diff(_sell: float, _buy: float) -> bool:
        if _sell - _buy < diff_min:
            return True
        return False

    try:
        sell_it = iter(bids_for_sell)
        sell_jpy, sell_qty, sell_price = next(sell_it)
        buy_it = iter(asks_for_buy)
        buy_jpy, buy_qty, buy_price = next(buy_it)
        while True:
            sell_qty -= sell_qty_adjustment
            if sell_qty < 0:
                sell_qty_adjustment = abs(sell_qty)
                sell_qty = 0
            buy_qty -= buy_qty_adjustment
            if buy_qty < 0:
                buy_qty_adjustment = abs(buy_qty)
                buy_qty = 0

            if sell_qty < buy_qty:
                jpy, qty, price = next(sell_it)
                if check_diff(jpy, buy_jpy):
                    break
                sell_jpy = jpy
                sell_qty += qty
                sell_price = price
            else:
                jpy, qty, price = next(buy_it)
                if check_diff(sell_jpy, jpy):
                    break
                buy_jpy = jpy
                buy_qty += qty
                buy_price = price
    except StopIteration:
        pass

    if math.isnan(sell_jpy) or math.isnan(buy_jpy):
        return None
    return dict(sell_jpy=sell_jpy,
                sell_price=sell_price,
                buy_jpy=buy_jpy,
                buy_price=buy_price,
                qty=min([sell_qty, buy_qty]),
                diff=sell_jpy - buy_jpy,
                diff_rate=(sell_jpy - buy_jpy) / buy_jpy)


def get_depth(asks_or_bids, qty: float):
    qty_sum = .0
    for x in asks_or_bids:
        qty_sum += x[1]
        if qty_sum >= qty:
            return x
    return None


def gen_number(start, stop, step, *, init=None, loop: bool = False):
    if step > 0:
        assert start < stop, (start, stop, step)
    elif step < 0:
        assert start > stop, (start, stop, step)

    number = init or start
    while True:
        while True:
            if step > 0 and number >= stop:
                break
            elif step < 0 and number <= stop:
                break
            yield number
            number += step
        if not loop:
            break
        number = start


def get_reverse_side(side: str) -> str:
    side = side.upper()
    return dict(SELL='BUY', BUY='SELL')[side]


def get_reverse_order_book_side(side: str) -> str:
    side = side.lower()
    return dict(asks='bids', bids='asks')[side]
