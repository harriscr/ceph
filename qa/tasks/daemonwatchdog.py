import logging
import signal
import time
from typing import Optional

from gevent import sleep
from gevent.event import Event
from gevent.greenlet import Greenlet

from tasks.feline import Feline

log = logging.getLogger(__name__)


class WoofError(Exception):
    """
    An exception to throw when the watchdog barks
    """

    def __init__(self, bark_reason: str) -> None:
        self.message = f"The WatchDog barked barked due to {bark_reason}"
        super().__init__(self.message)


class DaemonWatchdog(Greenlet):
    """
    DaemonWatchdog::

    Watch Ceph daemons for failures. If an extended failure is detected (i.e.
    not intentional), then the watchdog will unmount file systems and send
    SIGTERM to all daemons. The duration of an extended failure is configurable
    with watchdog_daemon_timeout.

    ceph:
      watchdog:
        daemon_restart [default: no]: restart daemon if "normal" exit (status==0).

        daemon_timeout [default: 300]: number of seconds a daemon
                                              is allowed to be failed before the
                                              watchdog will bark.
    """

    def __init__(self, ctx, config):
        super(DaemonWatchdog, self).__init__()
        self.config = ctx.config.get("watchdog", {})
        self.ctx = ctx
        self.e = None
        self.logger = log.getChild("daemon_watchdog")
        self.cluster = config.get("cluster", "ceph")
        self.name = "watchdog"
        self.stopping = Event()
        self.thrashers = ctx.ceph[config["cluster"]].thrashers
        self.cats: list[Feline] = ctx.ceph[config["cluster"]].felines
        self._exeption: Optional[Exception] = None

    def _run(self):
        try:
            self.watch()
        except Exception as e:
            # See _run exception comment for MDSThrasher
            self.e = e
            self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...

    def log(self, x):
        """Write data to logger"""
        self.logger.info(x)

    def stop(self):
        self.stopping.set()

    def bark(self, reason: str):
        self.log("BARK! unmounting mounts and killing all daemons")
        if hasattr(self.ctx, "mounts"):
            for mount in self.ctx.mounts.values():
                try:
                    mount.umount_wait(force=True)
                except:
                    self.logger.exception("ignoring exception:")
        daemons = []
        daemons.extend(
            filter(
                lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role("osd", cluster=self.cluster)
            )
        )
        daemons.extend(
            filter(
                lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role("mds", cluster=self.cluster)
            )
        )
        daemons.extend(
            filter(
                lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role("mon", cluster=self.cluster)
            )
        )
        daemons.extend(
            filter(
                lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role("rgw", cluster=self.cluster)
            )
        )
        daemons.extend(
            filter(
                lambda daemon: not daemon.finished(), self.ctx.daemons.iter_daemons_of_role("mgr", cluster=self.cluster)
            )
        )

        for daemon in daemons:
            try:
                daemon.signal(signal.SIGTERM)
            except:
                self.logger.exception("ignoring exception:")

        self.log(f"CHDEBUG: List of cats to kill is {self.cats}")
        for cat in self.cats:
            self.log(f"CHDEBUG: Killing cat {cat.collar}")
            cat.stop()

        self.log(f"CHDEBUG: List of thrashers to kill is {self.thrashers}")
        for thrasher in self.thrashers:
            self.log("CHDEBUG: Killing running thrasher {name}".format(name=thrasher.name))
            thrasher.stop_and_join()

        if self._exception:
            raise WoofError(reason) from self._exception
        else:
            raise WoofError(reason)

    def watch(self):
        self.log("watchdog starting")
        daemon_timeout = int(self.config.get("daemon_timeout", 300))
        daemon_restart = self.config.get("daemon_restart", False)
        bark_reason: str = ""
        daemon_failure_time = {}
        start_time = time.time()
        while not self.stopping.is_set():
            bark = False
            now = time.time()

            osds = self.ctx.daemons.iter_daemons_of_role("osd", cluster=self.cluster)
            mons = self.ctx.daemons.iter_daemons_of_role("mon", cluster=self.cluster)
            mdss = self.ctx.daemons.iter_daemons_of_role("mds", cluster=self.cluster)
            rgws = self.ctx.daemons.iter_daemons_of_role("rgw", cluster=self.cluster)
            mgrs = self.ctx.daemons.iter_daemons_of_role("mgr", cluster=self.cluster)

            daemon_failures = []
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), osds))
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), mons))
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), mdss))
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), rgws))
            daemon_failures.extend(filter(lambda daemon: daemon.finished(), mgrs))

            for daemon in daemon_failures:
                name = daemon.role + "." + daemon.id_
                dt = daemon_failure_time.setdefault(name, (daemon, now))
                assert dt[0] is daemon
                delta = now - dt[1]
                self.log("daemon {name} is failed for ~{t:.0f}s".format(name=name, t=delta))
                if delta > daemon_timeout:
                    bark = True
                    bark_reason = f"Daemon {name} failed"
                if daemon_restart == "normal" and daemon.proc.exitstatus == 0:
                    self.log(f"attempting to restart daemon {name}")
                    daemon.restart()

            # If a daemon is no longer failed, remove it from tracking:
            for name in list(daemon_failure_time.keys()):
                if name not in [d.role + "." + d.id_ for d in daemon_failures]:
                    self.log("daemon {name} has been restored".format(name=name))
                    del daemon_failure_time[name]

            for thrasher in self.thrashers:
                if thrasher.exception is not None:
                    # thrasher.stop_and_join()
                    bark_reason = f"Thrasher {thrasher.name} failed with Exception"
                    self.log(bark_reason)
                    self._exception = thrasher.exception
                    bark = True

            for cat in self.cats:
                if cat.exception is not None:
                    bark_reason = f"Cat {cat.collar} has asserted with {cat.exception}"
                    self.log(bark_reason)
                    self._exception = cat.exception
                    bark = True

            if time.time() - start_time >= 600:
                bark = True

            if bark:
                self.bark(bark_reason)
                return

            sleep(5)

        self.log("watchdog finished")
