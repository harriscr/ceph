"""
WatchedProcess process base class.

This can be applied to an object that we want the DaemonWatchdog to monitor
for failure, and bark when it sees an error
"""

from abc import ABCMeta, abstractmethod
from typing import Optional

from gevent.greenlet import Greenlet


class WatchedProcess(Greenlet, metaclass=ABCMeta):
    def __init__(self) -> None:
        self._exception: Optional[BaseException] = None

    @property
    def exception(self) -> Optional[BaseException]:
        return self._exception

    @property
    @abstractmethod
    def id(self) -> str:
        """
        Return a string identifier for this process
        """

    def set_exception(self, e: Exception) -> None:
        """
        Set the exception for this feline
        """
        self._exception = e

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the remote process running
        """
