# -*- coding: utf-8 -*-
import datetime

from shire.pool import Pool
from shire.workhorse import Workhorse
from tests.app.jobs import TestRestartJob
from tests.utils import TestBase
from shire.models import JobEntry


class TestJobRestart(TestBase):

    def setUp(self):
        self.redis.flushall()
        self.pool = Pool(config=self.config, name='test_pool')

    def test_run(self):
        job = TestRestartJob.delay(
            config=self.config,
            pool='test_pool',
            queue='abc',
            kwargs=dict(sleep=0.1),
        )
        workhorse = Workhorse(pool=self.pool, job_id=job.id)
        workhorse.run()
        job_entry = JobEntry.get(JobEntry.id == job.id)
        self.assertEquals(job_entry.status, JobEntry.STATUS_RESTART)

    def test_wait_minutes(self):
        WAIT_MINUTES = 10
        job = TestRestartJob.delay(
            config=self.config,
            pool='test_pool',
            queue='abc',
            kwargs=dict(wait_minutes=WAIT_MINUTES),
        )
        workhorse = Workhorse(pool=self.pool, job_id=job.id)
        started = datetime.datetime.now()
        workhorse.run()
        job_entry = JobEntry.get(JobEntry.id == job.id)
        self.assertEquals(job_entry.status, JobEntry.STATUS_RESTART)
        self._check_datetime_most_equal(
            job_entry.execute_at,
            started + datetime.timedelta(minutes=WAIT_MINUTES)
        )
