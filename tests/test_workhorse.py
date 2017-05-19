# -*- coding: utf-8 -*-

from shire.pool import Pool
from shire.workhorse import Workhorse
from tests.app.jobs import TestSleepJob
from tests.utils import TestBase


class TestWorkhorse(TestBase):

    def setUp(self):
        self.job = TestSleepJob.delay(
            config=self.config,
            pool='test_pool',
            queue='abc',
            kwargs=dict(sleep=0.1),
        )
        self.redis.flushall()
        self.pool = Pool(config=self.config, name='test_pool')

    def test_run(self):
        workhorse = Workhorse(pool=self.pool, job_id=self.job.id)
        workhorse.run()
        status = self.redis.get('test_job {}'.format(self.job.id))
        self.assertEquals(status.decode(), 'ENDED')
