# -*- coding: utf-8 -*-
import os
import signal
import time

import sys

from shire.logger import RedisLogger
from shire.redis_managers import PoolStatusManager, QueueManager
from shire.utils import check_pid_is_shire, create_console_handler, create_logger
from shire.workhorse import Daemon, Workhorse


__all__ = ['Pool']


class Pool(Daemon):
    SLEEP_TIME = 1
    MAX_WORKHORSES = None  # без ограничения

    def __init__(self, config, name, sleep_time=None, max_workhorses=None, verbose=False):
        super(Pool, self).__init__()
        self.config = config
        self.verbose = verbose
        self.redis = config.get_redis()
        self.name = name
        self.pool_logger = create_logger('shire.pool.{}'.format(name))
        if verbose:
            create_console_handler(self.pool_logger)
        self.sleep_time = sleep_time if sleep_time else self.SLEEP_TIME
        self.section = self.config.section_getter(self.config.POOL_SECTION)
        self.check_time = int(self.section.get(self.config.POOL_CHECK_TIME, self.config.POOL_CHECK_TIME_DEFAULT))
        self.max_workhorses = max_workhorses if max_workhorses else self.MAX_WORKHORSES
        self._queue_manager = QueueManager(connection=self.redis)
        self._children = []
        self.log = RedisLogger(config=self.config, pool=self.name)
        self._ps = PoolStatusManager(connection=self.redis)
        # когда форкуется workhorse, то она наследует signal.signal(signal.SIGTERM, self.terminate)
        # и надо проверять в  self.terminate кто сейчас завершается
        self.i_am_pool = True

    def pool_log(self, msg, level='info'):
        if not self.verbose:
            return
        getattr(self.pool_logger, level)(msg)

    def run(self):
        self.pool_log('Pool "{}" started'.format(self.name))
        self.pool_log('Max workhorses: {}'.format('unlimited' if self.max_workhorses is None else self.max_workhorses))
        self.pool_log('Redis check timeout: {}s'.format(self.check_time))
        self._ps.set_status(pool=self.name, _uuid=self.uuid, status=PoolStatusManager.STATUS_ACTIVE)
        signal.signal(signal.SIGTERM, self.terminate)
        while True:
            if not self.can_start_new_workhorse():
                time.sleep(self.sleep_time)
                continue
            while True:
                job_id = self._queue_manager.pop(pool=self.name, timeout=self.check_time)
                if self.status in (PoolStatusManager.STATUS_DEAD, PoolStatusManager.STATUS_KILL):
                    if job_id:
                        self._queue_manager.push(pool=self.name, job_id=job_id, to_tail=True)
                    if self.status == PoolStatusManager.STATUS_KILL:
                        self.kill_children()
                    self.terminate()
                if job_id:
                    # бывает None при выходе из brpop по таймауту
                    self._start_workhorse(job_id)  # тут пролетает sys.exit() от workhorse
                if not self.can_start_new_workhorse():
                    # проверяем после запуска задачи, что бы понять - можно ли выбирать другие задачи
                    break

    def _start_workhorse(self, job_id):
        self.pool_log('Workhorse for job #{} started'.format(job_id))
        workhorse = Workhorse(pool=self, job_id=job_id)
        pid = workhorse.fork()
        self._children.append(pid)

    @property
    def status(self):
        return self._ps.get_status(pool=self.name, _uuid=self.uuid)

    def wait_children(self):
        while self._get_children_count() > 0:
            time.sleep(self.sleep_time)
            if self.status == PoolStatusManager.STATUS_KILL:
                self.kill_children()
                return

    def kill_children(self):
        for child_pid in self._children:
            if check_pid_is_shire(child_pid):
                os.kill(child_pid, signal.SIGKILL)
        self._children = []

    def can_start_new_workhorse(self):
        children_count = self._get_children_count()
        if self.max_workhorses:
            return children_count < self.max_workhorses
        return True

    def _get_children_count(self):
        ended_pids = []
        try:
            while True:
                pid, ret_code = os.waitpid(-1, os.WNOHANG)
                if not pid:
                    break
                ended_pids.append(pid)
        except OSError:
            # нет child-процессов
            pass
        for child_pid in self._children[:]:
            if child_pid in ended_pids:
                self._children.remove(child_pid)
        return len(self._children)

    def terminate(self, sign=None, frame=None):
        if self.i_am_pool:
            self.wait_children()
            self._ps.set_status(pool=self.name, _uuid=self.uuid, status=PoolStatusManager.STATUS_TERMITATED)
        sys.exit(0)
