import logging
import pathlib
import re
import sys
import time
from pprint import pprint
from typing import List

from coinlib.utils.mixins import ThreadMixin
from docopt import docopt

from xrparb.arbconfig import CONFIG
from xrparb.dataprovider import DataProvider, FxProvider
from xrparb.arbbot import Bot


def main():
    args = docopt("""
    Usage:
      {f} [options]

    Options:
      --logging_level LEVEL  [default: INFO]
      --debug

    """.format(f=pathlib.Path(sys.argv[0]).name))
    pprint(args)
    params = {}
    for k, v in args.items():
        k = re.sub('^--', '', k)
        if isinstance(v, str):
            if v.isdigit():
                v = int(v)
            else:
                try:
                    v = float(v)
                except ValueError:
                    pass
        params[k] = v
    pprint(params)
    # params['debug'] = True
    # params['logging_level'] = 'DEBUG'
    logging.basicConfig(level=getattr(logging, params['logging_level']),
                        format='%(asctime)s|%(name)s|%(levelname)s: %(msg)s')
    runners = []  # type: List[ThreadMixin]
    fx_provider = FxProvider(CONFIG['fx_instruments'])
    runners += [fx_provider]
    data_provider = DataProvider(fx_provider=fx_provider,
                                 order_books=CONFIG['instruments'])
    runners += [data_provider]
    runners += [Bot(data_provider, **params)]
    try:
        for runner in runners:
            runner.start()
        while True:
            if any([not runner.is_active() for runner in runners]):
                break
            time.sleep(0.5)
    finally:
        for runner in runners:
            runner.stop()
        for runner in runners:
            runner.join()
    return
    start_wait_bot(**params)


def start_wait_bot(**params):
    bot = Bot(**params)
    try:
        bot.start().join()
    except KeyboardInterrupt:
        pass
    finally:
        bot.stop()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
