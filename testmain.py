from pprint import pprint

from coinlib.restapi import bitfinex2
from coinlib.utils import config


def main():
    credential = config.load()['bitfinex']['xrparb']
    api = bitfinex2.RestApi(credential['api_key'], credential['api_secret'])

    def get_trades():
        res = api.private_post('/auth/r/trades/tXRPUSD/hist', limit=501)
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
                'execution_id': d['id'],
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

    trades = get_trades()
    pprint(trades)
    print(len(trades))


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
