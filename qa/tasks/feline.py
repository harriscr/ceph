"""
Feline process base class.

This can be applied to an object that we want the DaemonWatchdog to monitor
for failure, and bark when it sees an error
"""

from abc import ABCMeta, abstractmethod
from typing import Optional


class Feline(metaclass=ABCMeta):
    def __init__(self) -> None:
        self._exception: Optional[Exception] = None

    @property
    def exception(self) -> Optional[Exception]:
        return self._exception

    @property
    @abstractmethod
    def collar(self) -> str:
        """
        The name for the feline
        """

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the Feline running
        """
