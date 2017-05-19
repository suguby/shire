# -*- coding: utf-8 -*-

import signal

from shire.redis_managers import QueueManager
from shire.models import Limit, Queue, JobEntry
from tests.app.jobs import TestSleepJob
from tests.utils import TestWithPid


class TestWhip(TestWithPid):
    FIRST_QUEUE = 'abc'
    FIRST_LIMIT = 2
    SECOND_QUEUE = 'bcd'
    SECOND_LIMIT = 3
    POOL = 'test_pool'
    POOL_LIMIT = FIRST_LIMIT + SECOND_LIMIT - 1

    def setUp(self):
        self.redis.flushall()
        self.config.save(self.config_path)

        # Создаем очереди
        Queue.create(name=self.FIRST_QUEUE, pool=self.POOL)
        Queue.create(name=self.SECOND_QUEUE, pool=self.POOL)

        # Указываем лимиты для очередей и пула (меньше суммы лимитов очередей)
        Limit.create(entity_type=Limit.ENTITY_QUEUE, entity=self.FIRST_QUEUE, limit=self.FIRST_LIMIT)
        Limit.create(entity_type=Limit.ENTITY_QUEUE, entity=self.SECOND_QUEUE, limit=self.SECOND_LIMIT)
        Limit.create(entity_type=Limit.ENTITY_POOL, entity=self.POOL, limit=self.POOL_LIMIT)
        self.valid_times = 0
        super(TestWhip, self).setUp()

    def test_functional(self):
        whip_process = self.popen_cli('run_whip')
        self.active_pid = whip_process.pid

        jobs = [
            TestSleepJob.delay(
                config=self.config, pool=self.POOL,
                queue=self.FIRST_QUEUE if i < 5 else self.SECOND_QUEUE,
                kwargs={'sleep': 1}
            )
            for i in range(10)
            ]

        def check_jobs():
            res = {'enqueued': [], self.FIRST_QUEUE: 0, self.SECOND_QUEUE: 0}
            for t in jobs:
                updated = JobEntry.get(JobEntry.id == t.id)
                if updated.status == JobEntry.STATUS_ENQUEUED:
                    res['enqueued'].append(updated.id)
                    res[updated.queue] += 1
            return res

        def validate(res):
            if len(res['enqueued']) == self.POOL_LIMIT:
                # Нужно убедиться, что whip в последующем не запустит ещё ненужных задач
                self.valid_times += 1
                if self.valid_times > 10:
                    return True
            return False

        # Проверяем, что было запущено ровно self.POOL_LIMIT задач
        jobs_result = self.check_for_timeout(
            callback=check_jobs, check_func=validate, message=u'Задачи установлены корректно'
        )

        # Проверяем корректность лимитов
        self.assertLessEqual(
            jobs_result[self.FIRST_QUEUE], self.FIRST_LIMIT, u'Установлены задачи согласно лимиту'
        )
        self.assertLessEqual(
            jobs_result[self.SECOND_QUEUE], self.SECOND_LIMIT, u'Установлены задачи согласно лимиту'
        )

        # Проверяем, что в редисе задачи так же поставлены
        redis_queue = QueueManager(connection=self.redis)
        in_queue = map(int, redis_queue.show_queue(self.POOL))
        self.assertEqual(set(jobs_result['enqueued']), set(in_queue), u'Задачи в redis соответствуют задачам в БД')

        # Завершаем whip
        whip_process.send_signal(signal.SIGTERM)

        # Проверка корректности завершения
        self.check_for_timeout(
            callback=lambda: self.check_pid(self.active_pid), message=u'Whip завершен некорректно'
        )
