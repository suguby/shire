# -*- coding: utf-8 -*-
import os
import signal
import sys
import time

import setproctitle

from shire.manager import ShireManager
from shire.pool import Pool


class PoolStarter(object):

    def __init__(self, config, pools=(), config_path=''):
        self.config = config
        self.pools = pools
        self.config_path = config_path

    def run(self):
        signal.signal(signal.SIGTERM, self.terminate)
        signal.signal(signal.SIGINT, self.terminate)
        for pool_name in self.pools:
            self.spawn_child(pool_name=pool_name)
        try:
            while True:
                time.sleep(1)
        except Exception as exc:
            self.terminate()

    def spawn_child(self, pool_name):
        # see https://stackoverflow.com/questions/972362/spawning-process-from-python/972383#972383

        try:
            pid = os.fork()
        except OSError as e:
            raise RuntimeError("1st fork failed: %s [%d]" % (e.strerror, e.errno))
        if pid != 0:
            # parent (calling) process is all done
            return pid
        # procname.setprocname('python -m shire.cli pool_starter FIRST child' + ' ' * 512)

        # detach from controlling terminal (to make child a session-leader)
        os.setsid()
        try:
            # Fork a second child and exit immediately to prevent zombies.  This
            # causes the second child process to be orphaned, making the init
            # process responsible for its cleanup.  And, since the first child is
            # a session leader without a controlling terminal, it's possible for
            # it to acquire one by opening a terminal in the future (System V-
            # based systems).  This second fork guarantees that the child is no
            # longer a session leader, preventing the daemon from ever acquiring
            # a controlling terminal.
            pid = os.fork()
        except OSError as e:
            raise RuntimeError("2nd fork failed: %s [%d]" % (e.strerror, e.errno))
        if pid != 0:
            # child process is all done
            os._exit(0)
        # procname.setprocname('python -m shire.cli pool_starter SECOND child' + ' ' * 512)

        # grandchild process now non-session-leader, detached from parent
        # grandchild process must now close all open files
        try:
            maxfd = os.sysconf("SC_OPEN_MAX")
        except (AttributeError, ValueError):
            maxfd = 1024

        for fd in range(maxfd):
            try:
                os.close(fd)
            except OSError:  # ERROR, fd wasn't open to begin with (ignored)
                pass

        # redirect stdin, stdout and stderr to /dev/null
        os.open(os.devnull, os.O_RDWR)  # standard input (0)
        os.dup2(0, 1)
        os.dup2(0, 2)

        # and finally let's execute the executable for the daemon!
        cmd = 'shire.cli --config={} run_pool --name {}'.format(self.config_path, pool_name)
        setproctitle.setproctitle(cmd + ' ' * 512)
        pool = Pool(config=self.config, name=pool_name)
        try:
            pool.run()
        except Exception as e:
            pool.log.exception('Pool name {} uuid{}'.format(pool.name, pool.uuid))
            os._exit(1)
        else:
            os._exit(0)

    def terminate(self, sign=None, frame=None):
        manager = ShireManager(config=self.config)
        manager.terminate_pools(self.pools)
        sys.exit(0)

