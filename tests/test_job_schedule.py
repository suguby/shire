# -*- coding: utf-8 -*-
import datetime
import json

from shire.models import Crontab, JobEntry
from tests.app import jobs
from tests.utils import TestBase


class TestShedule(TestBase):
    KEY = 'key1'
    CRON_STRING = '*/10 * * * *'
    POOL = 'test_pool'
    QUEUE = 'test_queue'

    def tearDown(self):
        Crontab.truncate_table()

    def test_first_run(self):
        jobs.TestScheduledJob.schedule(config=self.config, key=self.KEY, cron_string=self.CRON_STRING,
                                       pool=self.POOL, queue=self.QUEUE, kwargs=dict(param=1))
        crontabs = Crontab.select()
        self.assertEqual(len(crontabs), 1)
        for ct in crontabs:
            self.assertEqual(ct.key, self.KEY)
            self.assertEqual(ct.cron_string, self.CRON_STRING)
            self.assertEqual(ct.pool, self.POOL)
            self.assertEqual(ct.queue, self.QUEUE)
            self.assertEqual(ct.host, JobEntry.HOST_DEFAULT)
            self.assertEqual(ct.func_call[JobEntry.FUNC_CALL_PATH], jobs.__file__)
            self.assertEqual(ct.func_call[JobEntry.FUNC_CALL_CLASS], jobs.TestScheduledJob.__name__)
            self.assertTrue('param' in ct.func_call[JobEntry.FUNC_CALL_KWARGS])
            self.assertEqual(ct.func_call[JobEntry.FUNC_CALL_KWARGS]['param'], 1)
            self.assertEqual(ct.last_scheduled, None)

    def test_other_runs(self):
        Crontab.create(
            key=self.KEY,
            cron_string='1 1 1 1 *',
            func_call_=json.dumps({}),
            pool='some_pool',
            queue='some_queue',
            host='some_host',
        )
        self.test_first_run()

    def test_change_func_call(self):
        # создаем запись идентичную test_first_run, но с другим func_call
        crontab = Crontab.create(
            key=self.KEY,
            cron_string=self.CRON_STRING,
            func_call_=json.dumps({}),
            pool=self.POOL,
            queue=self.QUEUE,
            last_scheduled=datetime.datetime.now(),
        )
        crontab.func_call = JobEntry.make_func_call(
            file_path=__file__,  # изменяем что-то внутри func_call
            file_cls=jobs.TestScheduledJob.__name__,
            args=(),
            kwargs=dict(param=1),
        )
        crontab.save()
        self.test_first_run()
