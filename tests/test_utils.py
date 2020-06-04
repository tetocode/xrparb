import pytest

from coinarb import utils
from coinarb.utils import gen_number


def test_adjust_asks_bids():
    asks = []
    bids = [(9, 10), (8, 20)]
    asks, bids = utils.adjust_asks_bids(asks, bids, 1)
    assert asks == []
    assert bids == [(9, 10, 9), (8, 20, 8)]

    asks = [(10, 10), (11, 20)]
    bids = []
    asks, bids = utils.adjust_asks_bids(asks, bids, 1)
    assert asks == [(10, 10, 10), (11, 20, 11)]
    assert bids == []

    asks = [(10, 10), (11, 20)]
    bids = [(9, 10), (8, 20)]
    asks, bids = utils.adjust_asks_bids(asks, bids, 1)
    assert asks == [(10, 10, 10), (11, 20, 11)]
    assert bids == [(9, 10, 9), (8, 20, 8)]

    asks = [(10, 10), (11, 20)]
    bids = [(10, 10), (8, 20)]
    asks, bids = utils.adjust_asks_bids(asks, bids, 1)
    assert asks == [(11, 20, 11)]
    assert bids == [(8, 20, 8)]

    asks = [(10, 10), (11, 20)]
    bids = [(10, 20), (8, 20)]
    asks, bids = utils.adjust_asks_bids(asks, bids, 1)
    assert asks == [(11, 20, 11)]
    assert bids == [(10, 10, 10), (8, 20, 8)]

    asks = [(10, 10), (11, 20), (12, 30)]
    bids = [(11, 20), (8, 20)]
    asks, bids = utils.adjust_asks_bids(asks, bids, 1.5)
    assert asks == [(11 * 1.5, 10, 11), (12 * 1.5, 30, 12)]
    assert bids == [(8 * 1.5, 20, 8)]


def test_calculate_diff():
    bids = [(9, 10), (8, 20)]
    asks, bids = utils.adjust_asks_bids([], bids, 1)
    sell_order_book = dict(asks=asks, bids=bids)
    asks = [(10, 10), (11, 20)]
    asks, bids = utils.adjust_asks_bids(asks, [], 1)
    buy_order_book = dict(asks=asks, bids=bids)
    result = utils.calculate_diff(sell_order_book, buy_order_book, 1)
    assert result['diff'] == -1

    bids = [(11, 10), (8, 20)]
    asks, bids = utils.adjust_asks_bids([], bids, 1.5)
    sell_order_book = dict(asks=asks, bids=bids)
    asks = [(10, 10), (11, 20)]
    asks, bids = utils.adjust_asks_bids(asks, [], 1.5)
    buy_order_book = dict(asks=asks, bids=bids)
    result = utils.calculate_diff(sell_order_book, buy_order_book, 1)
    assert result['diff'] == 1 * 1.5
    assert result['buy_jpy'] == 10 * 1.5
    assert result['buy_price'] == 10
    assert result['sell_jpy'] == 11 * 1.5
    assert result['sell_price'] == 11
    assert result['qty'] == 10

    bids = [(12, 1), (11.5, 1), (11, 10), (8, 20)]
    asks, bids = utils.adjust_asks_bids([], bids, 1.5)
    sell_order_book = dict(asks=asks, bids=bids)
    asks = [(10, 10), (11, 20)]
    asks, bids = utils.adjust_asks_bids(asks, [], 1.5)
    buy_order_book = dict(asks=asks, bids=bids)
    result = utils.calculate_diff(sell_order_book, buy_order_book, 1, sell_qty_adjustment=5)
    assert result['diff'] == 1 * 1.5
    assert result['buy_jpy'] == 10 * 1.5
    assert result['buy_price'] == 10
    assert result['sell_jpy'] == 11 * 1.5
    assert result['sell_price'] == 11
    assert result['qty'] == 7

    bids = [(11, 10), (8, 20)]
    asks, bids = utils.adjust_asks_bids([], bids, 1.5)
    sell_order_book = dict(asks=asks, bids=bids)
    asks = [(8, 1), (9, 2), (10, 10), (11, 20)]
    asks, bids = utils.adjust_asks_bids(asks, [], 1.5)
    buy_order_book = dict(asks=asks, bids=bids)
    result = utils.calculate_diff(sell_order_book, buy_order_book, 1, buy_qty_adjustment=5)
    assert result['diff'] == 1 * 1.5
    assert result['buy_jpy'] == 10 * 1.5
    assert result['buy_price'] == 10
    assert result['sell_jpy'] == 11 * 1.5
    assert result['sell_price'] == 11
    assert result['qty'] == 8


def test_gen_number():
    assert list(gen_number(1, 3, 1)) == [1, 2]
    assert list(gen_number(3, 1, -1)) == [3, 2]

    with pytest.raises(AssertionError):
        list(gen_number(1, 3, -1))
    with pytest.raises(AssertionError):
        list(gen_number(3, 1, 1))

    g = gen_number(1, 3, 1, loop=True)
    assert next(g) == 1
    assert next(g) == 2
    assert next(g) == 1
    assert next(g) == 2

    assert list(gen_number(1.1, 1.3, 0.1)) == [1.1, 1.1 + 0.1]

    assert list(gen_number(1, 4, 1, init=2)) == [2, 3]
