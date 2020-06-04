import time
from typing import Dict, Iterable

from coinlib.utils.mixins import LoggerMixin, ThreadMixin

from coinarb.agents.agent import Agent
from coinarb.dataprovider import DataProvider


class Task(LoggerMixin, ThreadMixin):
    def __init__(self, agents: Iterable[Agent], data_provider: DataProvider, config: dict, **__):
        self._logger = self._make_logger()
        self._thread_data = self._make_thread_data()
        self.agents = {agent.name: agent for agent in agents}  # type: Dict[str, Agent]
        self._data_provider = data_provider
        self.config = config

    def get_updated_order_books(self):
        return self._data_provider.get_updated_order_books()

    def now(self) -> float:
        _ = self
        return time.time()

    def sleep(self, seconds: float):
        _ = self
        time.sleep(seconds)
