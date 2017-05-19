# -*- coding: utf-8 -*-
from __future__ import print_function
import time

from shire.job import Job


class BaseTestJob(Job):

    def __init__(self, workhorse):
        super(BaseTestJob, self).__init__(workhorse=workhorse)
        self.redis = self.workhorse.config.get_redis()


class TestSleepJob(BaseTestJob):

    def run(self, sleep=None):
        self.redis.set('test_job {}'.format(self.job_entry.id), 'STARTED')
        if sleep:
            time.sleep(sleep)
        self.redis.set('test_job {}'.format(self.job_entry.id), 'ENDED')


class TestRestartJob(BaseTestJob):

    def run(self, sleep=None, wait_minutes=None):
        self.redis.set('restart_job {}'.format(self.job_entry.id), 'STARTED')
        if sleep:
            time.sleep(sleep)
        self.restart(wait_minutes=wait_minutes)


class TestScheduledJob(Job):

    def run(self, sleep=None):
        print('Hello, world!')


class TestOtherModuleJob(BaseTestJob):

    def run(self):
        # этот модуль будет лежать по отдельному пути
        import other_module
        other_module.run()
        self.redis.set('test_job {}'.format(self.job_entry.id), 'ENDED')


class TestVirtualEnvJob(BaseTestJob):

    def run(self):
        pass
