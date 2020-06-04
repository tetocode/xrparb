import threading

from . import task


class Task(task.Task):
    def run(self):
        if self.config['pre_cancel']:
            l, h = self.config['pre_cancel_delta_range']
            agent = self.agents['quoinex']
            while self.is_active():
                try:
                    with agent.get_client() as client:
                        order_book = client.get_order_book('XRP_JPY')
                    price = (order_book['asks'][0][0] + order_book['bids'][0][0]) / 2
                    agent.cancel_matched_orders('XRP_JPY', lambda v: (price + l) <= v['price'] <= (price + h))
                except Exception as e:
                    self.logger.exception(e)
                    self.sleep(10)
        self.run_xrp_jpy_maker()

    def run_xrp_jpy_maker(self):
        """
        'entries': [...]
        """
        for i, entry_config in enumerate(self.config['entries']):
            if entry_config['enable']:
                threading.Thread(target=self.run_entry,
                                 args=(entry_config,)).start()

    def run_entry(self, entry_config: dict):
        check_interval = entry_config['check_interval']  # type: float
        cv = threading.Condition()
        threads = []
        for i, item_config in enumerate(entry_config['items']):
            thread = threading.Thread(target=self.run_entry_item,
                                      args=(entry_config, item_config, cv))
            thread.start()
            threads.append(thread)
        while self.is_active():
            with cv:
                cv.notify()
            self.sleep(check_interval)
        for thread in threads:
            pass
        with cv:
            cv.notify_all()

    def run_entry_item(self, entry_config:dict):
        pass