import json
import pathlib
import sys
import threading
import time
from pprint import pprint

from coinlib import bitfinex2
from coinlib.utils import config
from docopt import docopt

from xrparb.credentialpool import CredentialPool


def main():
    args = docopt("""
    Usage:
      {f} [options] [--] inject
      {f} [options] detail
      {f} [options] list
      {f} [options] test

    Options:
      --logging_level LEVEL  [default: INFO]
      --debug

    """.format(f=pathlib.Path(sys.argv[0]).name))
    pprint(args)

    def get_v1_active_orders():
        import coinlib.restapi.bitfinex2
        api = coinlib.restapi.bitfinex2.RestApi(credential['api_key'], credential['api_secret'])
        fields = [
            'id', 'gid', 'cid', 'symbol', 'mts_create', 'mts_update',
            'amount', 'amount_orig', 'type', 'type_prev',
            '_0', '_1',
            'flags', 'status',
            '_2', '_3',
            'price', 'price_avg', 'price_trailing', 'price_aux_limit',
            '_4', '_5', '_6',
            'notify', 'hidden', 'placed_id',
        ]
        # v2_orders = [dict(zip(fields, x)) for x in api.private_post('/auth/r/orders/tXRPUSD')]
        return {v['id']: v for v in client.private_post('/orders')}

    credential_pool = CredentialPool(config.load()['bitfinex2']['credentials'])
    with credential_pool.get() as credential:
        client = bitfinex2.StreamClient(credential['api_key'], credential['api_secret'])
        with client:
            client.subscribe(account=True)
            client.wait_authentication(timeout=10)
            account = client.account
            if args['list']:
                for _ in range(100):
                    if account.orders:
                        break
                    time.sleep(0.2)
                v1_orders = get_v1_active_orders()
                for order in sorted(account.orders.values(), key=lambda x: x['order_id']):
                    v1_order = v1_orders.get(order['order_id'], {})
                    d = {}
                    d.update(v1_order)
                    d.update(order)
                    print('#{id} price={price} amount={original_amount} {type} {side} hidden={is_hidden} {flags}'.format(**d))
                return
            if args['detail']:
                for _ in range(100):
                    if account.orders:
                        break
                    time.sleep(0.2)
                v1_orders = get_v1_active_orders()
                for order in sorted(account.orders.values(), key=lambda x: x['order_id']):
                    v1_order = v1_orders.get(order['order_id'])
                    print('#v1', json.dumps(v1_order, sort_keys=True))
                    print('#v2', json.dumps(order, sort_keys=True))
                return
            if args['inject']:
                for _ in range(100):
                    if account.orders:
                        break
                    time.sleep(0.2)
                v1_orders = get_v1_active_orders()
                for order in account.orders.values():
                    v1_order = v1_orders.get(order['order_id'])
                    if not v1_order or order['_data']['gid']:
                        continue
                    if bool(order['_data']['flags'] & 64) and not v1_order['is_hidden']:
                        side = v1_order['side'].lower()
                        if side == 'sell':
                            op = account.update_order_op(order['order_id'], gid=1, flags=[])
                            print('#update', json.dumps(op, sort_keys=True))
                            account.submit_order_op(op)
                        if side == 'buy':
                            op = account.update_order_op(order['order_id'], gid=2, flags=[])
                            print('#update', json.dumps(op, sort_keys=True))
                            account.submit_order_op(op)
                return
            if args['test']:
                from coinlib.coinlib.bitfinex2 import Flag
                op = account.new_order_op('XRP_USD', 'LIMIT', 'SELL', price=1, qty=25, gid=3)
                print('#new', json.dumps(op, sort_keys=True))
                order_id = account.submit_order_op(op)
                time.sleep(10)
                for _ in range(100):
                    print('#', _)
                    op1 = account.update_order_op(order_id, flags=[Flag.POST_ONLY, Flag.HIDDEN])
#                    op2 = account.update_order_op(order_id, price=0.5, flags=[Flag.POST_ONLY])
 #                   account.submit_order_op(op2, async=True)
                    account.submit_order_op(op1)
                    # t1 = threading.Thread(target=client.account.submit_order_op, args=(op1,), daemon=True)
                    # t2 = threading.Thread(target=client.account.submit_order_op, args=(op2,), daemon=True)
                    # threads = [t1, t2]
                    # for t in threads:
                    #     t.start()
                    # for t in threads:
                    #     t.join()
                    # op1 = account.update_order_op(order_id, flags=[Flag.POST_ONLY, Flag.HIDDEN])
                    # op2 = account.update_order_op(order_id, flags=[Flag.POST_ONLY])
                    # client.account.submit_order_op(op1)
                    # client.account.submit_order_op(op2)
                    time.sleep(0.5)
                    v1_order = client.get_order('', order_id)
                    if not v1_order['is_hidden']:
                        if bool(client.account.orders[order_id]['_data']['flags'] & 64):
#                                if bool(client2.account.orders[order_id]['_data']['flags'] & 64):
                                print('#somehing happened')
                                break
                    time.sleep(1)
                time.sleep(2)
                op2 = account.update_order_op(order_id)
                account.submit_order_op(op2, async=True)
                time.sleep(100)
                return


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
