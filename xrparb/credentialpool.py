import contextlib
import logging
from queue import Queue, Empty
from typing import Iterable

from coinlib.utils.mixins import LoggerMixin


class CredentialTimeout(BaseException):
    pass


class CredentialPool(LoggerMixin):
    TIMEOUT = 600

    def __init__(self, credentials: Iterable[dict] = None, logger: logging.Logger = None, timeout: float = None):
        self._q = Queue()
        self._logger = self._make_logger(logger)
        self.timeout = timeout or self.TIMEOUT
        self.add_credentials(credentials or [])

    def add_credentials(self, credentials: Iterable[dict]):
        for credential in credentials:
            self._q.put(credential)

    @contextlib.contextmanager
    def get(self) -> dict:
        try:
            credential = self._q.get(timeout=self.timeout)
        except Empty:
            raise CredentialTimeout('cant get credential')
        try:
            yield credential
        finally:
            self._q.put(credential)
