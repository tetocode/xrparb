CONFIG = {
    'instruments': [
        ('bitbankcc', 'XRP_JPY'),
        ('bitfinex2', 'XRP_USD'),
    ],
    'fx_instruments': [
        'USD_JPY',
    ],
    'bitbankcc': {
        'funds': {
            'JPY': dict(locked=0),
            'XRP': dict(locked=0),
        },
        'precisions': {
            'XRP_JPY': dict(price=3, qty=4)
        }
    },
    'bitfinex2': {
        'precisions': {
            'XRP_USD': dict(price=5, qty=8)
        },
    },
    'sell': {
        'gid': 1,
        'position_max': 220000,
        'jpy_diff_min': 1,
        'order_qty': 100,
        'unit_n': 1,
        'diff_step': 0.001,
        'jpy_diff_decrement': 0.0001,
        'jpy_diff_reentry_adjustment': 0.1,
        'interval': 1,
        'depth': 5000,
    },
    'buy': {
        'gid': 2,
        'position_max': 48000,
        'jpy_diff_min': 1,
        'order_qty': 100,
        'unit_n': 2,
        'diff_step': 0.001,
        'jpy_diff_decrement': 0.0001,
        'jpy_diff_reentry_adjustment': 0.1,
        'interval': 1,
        'depth': 5000,
    },
    'shifter': {
        'diff_max_init': 0.45,
        'diff_min_init': -0.45,
        'execution_shift_rate': 0.01 / 1000,
        'shift_step': 0.0002,
    },
    'order_update_interval': 0.5,
}

try:
    from . import arbconfiglocal

    CONFIG.update(arbconfiglocal.CONFIG)
except ImportError:
    pass
