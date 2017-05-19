# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import subprocess

import signal

import six

from shire.redis_managers import QueueManager
from shire.utils import decode_if_not_empty
from tests.test_pool import TestPoolBase


class TestPoolStarter(TestPoolBase):

    def tearDown(self):
        super(TestPoolStarter, self).tearDown()
        for row in subprocess.check_output(['ps', 'aux']).split(six.b('\n')):
            if isinstance(row, six.binary_type):
                row = row.decode(errors='replace')
            if 'shire.cli' in row:
                pid = row.split()[1]
                try:
                    os.kill(int(pid), signal.SIGKILL)
                    print('shire.cli with pid {} was KILLED!'.format(pid))
                except OSError as e:
                    pass

    def test_functional(self):
        job_sleep_time = 2
        pool_names = ['test1', 'test2']
        #  стартуем пулы
        pool_starter = self._start_subproc('start_pools', '--names='+','.join(pool_names))
        uuids = []
        for pool_name in pool_names:
            _uuid = self._get_pool_uuid(pool_name=pool_name)
            uuids.append([pool_name, _uuid])
        # ставим по задачке в каждую очередь и ждем что они начали выполнятся
        jobs = []
        for pool_name in pool_names:
            job = self._job_to_queue(pool_name, job_sleep_time=job_sleep_time)
            jobs.append(job)
        self.check_for_timeout(
            callback=lambda: decode_if_not_empty(self.redis.get('test_job {}'.format(jobs[-1].id))),
            check_func=lambda value: value == 'STARTED',
            timeout=job_sleep_time * 2,
        )
        # завершаем стартер
        pool_starter.terminate()
        pool_starter.wait()
        # ставим по новой задачке в каждую очередь
        jobs_2 = []
        for pool_name in pool_names:
            job = self._job_to_queue(pool_name, job_sleep_time=job_sleep_time)
            jobs_2.append(job)
        # ждем пока выполнятся первые задачи
        for job in jobs:
            self.check_for_timeout(
                callback=lambda: decode_if_not_empty(self.redis.get('test_job {}'.format(job.id))),
                check_func=lambda value: value == 'ENDED',
                timeout=job_sleep_time * 2,
            )
        # ждем пока помрут пулы
        for pool_name, _uuid in uuids:
            self.check_for_timeout(
                callback=lambda: decode_if_not_empty(self.pool_status_manager.get_status(pool=pool_name, _uuid=_uuid)),
                check_func=lambda value: value == self.pool_status_manager.STATUS_TERMITATED,
                timeout=job_sleep_time * 2,
            )
        # проверяем что вторые таски опять в очереди
        redis_queue = QueueManager(connection=self.redis)
        for job in jobs_2:
            enqueued_job_id = redis_queue.pop(job.pool)
            self.assertEquals(str(job.id), enqueued_job_id)

