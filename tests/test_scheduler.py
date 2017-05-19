# -*- coding: utf-8 -*-
import datetime

import croniter

from shire.models import JobEntry, Crontab
from shire.pool import Pool
from shire.scheduler import CrontabJob
from shire.workhorse import Workhorse
from tests.app import jobs
from tests.utils import TestBase


class TestScheduler(TestBase):
    ONCE_AT_MINUTES = 10

    def setUp(self):
        self.pool = Pool(config=self.config, name='test_pool')
        self.cron_string = '*/{} * * * *'.format(self.ONCE_AT_MINUTES)

        self.crontab = Crontab(
            key='test_crontab_job',
            cron_string=self.cron_string,
            pool=self.pool.name,
            queue='default',
            host='default',
        )
        self.test_job_name = jobs.TestScheduledJob.__name__
        self.crontab.func_call = JobEntry.make_func_call(file_path=jobs.__file__, file_cls=self.test_job_name)
        self.crontab.save()

        self.crontab_job = CrontabJob.start_crontab(config=self.config, pool=self.pool.name)
        self.started_at = datetime.datetime.now()

    def tearDown(self):
        Crontab.truncate_table()
        JobEntry.truncate_table()

    def test_first_run(self):
        workhorse = Workhorse(pool=self.pool, job_id=self.crontab_job.id)
        workhorse.run()

        next_scheduler_jobs = JobEntry.select().where(
            (JobEntry.execute_at > self.started_at) &
            (JobEntry.queue == CrontabJob.CRONTAB_QUEUE)
        )
        self.assertEquals(len(next_scheduler_jobs), 1)
        self._check_datetime_most_equal(
            dt1=next_scheduler_jobs[0].execute_at,
            dt2=self.started_at + datetime.timedelta(minutes=CrontabJob.RERUN_PERIOD_MINUTES))

        scheduled_jobs = JobEntry.select().where(
            (JobEntry.execute_at > self.started_at) &
            (JobEntry.queue != CrontabJob.CRONTAB_QUEUE)
        ).order_by(JobEntry.execute_at)
        self.assertEquals(len(scheduled_jobs), CrontabJob.SCHEDULE_DELTA_MINUTES / self.ONCE_AT_MINUTES)
        prev_start_time = None
        for job in scheduled_jobs:
            self.assertEquals(job.func_call[JobEntry.FUNC_CALL_CLASS], self.test_job_name)
            if prev_start_time:
                self._check_datetime_most_equal(
                    dt1=prev_start_time + datetime.timedelta(minutes=self.ONCE_AT_MINUTES),
                    dt2=job.execute_at,
                )
            prev_start_time = job.execute_at

    def test_other_run(self):
        # ставим задачи на половинное время в будущее - как буд-то они были поставлены в прошлый раз
        schedule_from = self.started_at
        schedule_until = self.started_at + datetime.timedelta(
            minutes=(CrontabJob.SCHEDULE_DELTA_MINUTES - CrontabJob.RERUN_PERIOD_MINUTES))
        self._set_sheduled_jobs(schedule_from, schedule_until)
        # проверяем что "как будто первый запуск"
        self.test_first_run()

    def test_restart(self):
        TIME_SHIFT = datetime.timedelta(minutes=13)
        # ставим задачи, как будто шедуллер отработал TIME_SHIFT минут назад
        schedule_from = self.started_at - TIME_SHIFT
        schedule_until = schedule_from + datetime.timedelta(minutes=CrontabJob.SCHEDULE_DELTA_MINUTES)
        self._set_sheduled_jobs(schedule_from, schedule_until)
        # плюс запуск кронтаба в будущем
        future_crontab = CrontabJob.delay(config=self.config, pool=self.pool.name, queue=CrontabJob.CRONTAB_QUEUE)
        future_crontab.execute_at = schedule_from + datetime.timedelta(minutes=CrontabJob.RERUN_PERIOD_MINUTES)
        future_crontab.save()
        # проверяем что "как будто первый запуск"
        self.test_first_run()

    def _set_sheduled_jobs(self, schedule_from, schedule_until):
        croniter_obj = croniter.croniter(
            expr_format=self.cron_string,
            start_time=schedule_from,
            ret_type=datetime.datetime
        )
        last_run = None
        next_run = croniter_obj.get_next()
        while next_run <= schedule_until:
            job_entry = jobs.TestScheduledJob.delay(config=self.config, pool=self.pool.name, queue='default')
            last_run = job_entry.execute_at = next_run
            job_entry.save()
            next_run = croniter_obj.get_next()
        self.crontab.last_scheduled = last_run
        self.crontab.save()
        return last_run
