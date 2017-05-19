# -*- coding: utf-8 -*-

import datetime

import croniter

from shire.job import Job
from shire.models import Crontab, JobEntry
from shire.utils import create_logger


class CrontabJob(Job):
    SCHEDULE_DELTA_MINUTES = 60
    RERUN_PERIOD_MINUTES = 30
    CRONTAB_QUEUE = '__shire_crontab__'

    def __init__(self, workhorse):
        self.logger = create_logger('shire.schedule')
        super(CrontabJob, self).__init__(workhorse=workhorse)

    def run(self):
        start = datetime.datetime.now()
        for crontab in Crontab.select():
            schedule_from = crontab.last_scheduled if crontab.last_scheduled else start
            schedule_until = start + datetime.timedelta(minutes=self.SCHEDULE_DELTA_MINUTES)
            if schedule_until <= schedule_from:
                continue
            croniter_obj = croniter.croniter(
                expr_format=crontab.cron_string,
                start_time=schedule_from,
                ret_type=datetime.datetime
            )
            next_run = croniter_obj.get_next()
            while next_run <= schedule_until:
                try:
                    JobEntry.create(
                        func_call_=crontab.func_call_,
                        pool=crontab.pool,
                        queue=crontab.queue,
                        host=crontab.host,
                        execute_at=next_run,
                    )
                    crontab.last_scheduled = next_run
                    crontab.save()
                except Exception as e:
                    self.logger.error(u'Ошибка во время запуска задачи #%s', crontab.id)
                    self.logger.exception(e)
                else:
                    self.logger.info(u'Задача #%s успешно поставлена на выполнение в %s', crontab.id, next_run)
                next_run = croniter_obj.get_next()

        future_crontab_jobs = list(JobEntry.select().where(
            (JobEntry.execute_at > start) &
            (JobEntry.queue == CrontabJob.CRONTAB_QUEUE)
        ).order_by(JobEntry.execute_at))
        if len(future_crontab_jobs):
            last_job = future_crontab_jobs.pop()
            last_job.execute_at = start + datetime.timedelta(minutes=self.RERUN_PERIOD_MINUTES)
            last_job.save()
            for job in future_crontab_jobs:
                job.delete_instance()
        else:
            self.start_crontab(config=self.workhorse.config, pool=self.workhorse.pool.name,
                               wait_minutes=self.RERUN_PERIOD_MINUTES)

    @classmethod
    def start_crontab(cls, *args, **kwargs):
        kwargs['queue'] = cls.CRONTAB_QUEUE
        return cls.delay(*args, **kwargs)

    @classmethod
    def stop_crontab(cls, config):
        with config.with_db():
            JobEntry.delete().where(JobEntry.queue == cls.CRONTAB_QUEUE).execute()

    @classmethod
    def clear_jobs(cls, config):
        with config.with_db():
            JobEntry.delete().where(
                (JobEntry.execute_at > datetime.datetime.now()) &
                (JobEntry.queue != cls.CRONTAB_QUEUE)
            ).execute()

    @classmethod
    def list(cls, config):
        with config.with_db():
            return Crontab.select()
