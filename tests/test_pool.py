# -*- coding: utf-8 -*-
import signal
import time

from shire.redis_managers import QueueManager, PoolStatusManager
from shire.utils import decode_if_not_empty
from tests.app.jobs import TestSleepJob
from tests.utils import TestBase


class TestPoolBase(TestBase):

    def setUp(self):
        self.redis.flushall()
        self.config[self.config.POOL_SECTION][self.config.POOL_CHECK_TIME] = '1'
        self.config.save(self.config_path)
        self.pool_status_manager = PoolStatusManager(connection=self.redis)
        self.subprocesses = []

    def tearDown(self):
        for subproc in self.subprocesses:
            if hasattr(subproc, 'pid') and self.check_pid(subproc.pid):
                subproc.kill()

    def _start_subproc(self, *args):
        proc = self.popen_cli(*args)
        self.subprocesses.append(proc)
        return proc

    def _job_to_queue(self, pool_name, job_sleep_time):
        job = TestSleepJob.delay(config=self.config, pool=pool_name, queue='abc',
                                 kwargs=dict(sleep=job_sleep_time))
        redis_queue = QueueManager(connection=self.redis)
        redis_queue.push(job.pool, job.id)
        return job

    def _get_pool_uuid(self, pool_name, timeout=60):
        _uuid, iteration_count = None, 0
        while not _uuid:
            for row in self.pool_status_manager.get_all(pool=pool_name):
                _uuid = row.split(':')[-1]
                break
            time.sleep(0.1)
            iteration_count += 1
            if iteration_count > timeout / 0.1:
                raise Exception("Can't get pool_uuid after {} secs".format(timeout))
        return _uuid


class TestPool(TestPoolBase):

    def test_functional(self):
        #  стартуем пул
        pool_name = 'test_pool'
        self.pool_process = self._start_subproc('run_pool', '--name='+pool_name)
        _uuid = self._get_pool_uuid(pool_name=pool_name)
        #  задачку в очередь
        job_sleep_time = 1
        job = self._job_to_queue(pool_name, job_sleep_time)
        #  проверяем что она началась
        self.check_for_timeout(
            callback=lambda: decode_if_not_empty(self.redis.get('test_job {}'.format(job.id))),
            check_func=lambda value: value == 'STARTED',
        )
        #  ждем пока задачка выполнится
        time.sleep(job_sleep_time)
        self.check_for_timeout(
            callback=lambda: decode_if_not_empty(self.redis.get('test_job {}'.format(job.id))),
            check_func=lambda value: value == 'ENDED',
        )
        # киляем пул
        self.pool_process.send_signal(signal.SIGTERM)
        #  проверяем что пул завершился с нужным статусом
        ret_code = self.pool_process.wait()
        self.assertEqual(ret_code, 0)
        pool_status = self.pool_status_manager.get_status(pool='test_pool', _uuid=_uuid)
        self.assertEqual(pool_status, PoolStatusManager.STATUS_TERMITATED)

    def test_dead_state(self):
        #  стартуем пул
        pool_name = 'test_pool'
        self.pool_process = self._start_subproc('run_pool', '--name='+pool_name)
        _uuid = self._get_pool_uuid(pool_name=pool_name)

        job_sleep_time = 2
        #  ставим первую задачку
        job_1 = self._job_to_queue(pool_name, job_sleep_time)
        #  ожидаем, пока она начнет выполнятся
        self.check_for_timeout(
            callback=lambda: decode_if_not_empty(self.redis.get('test_job {}'.format(job_1.id))),
            check_func=lambda value: value == 'STARTED',
        )
        #  ставим статус "умри"
        self.pool_status_manager.set_status(pool='test_pool', _uuid=_uuid, status=PoolStatusManager.STATUS_DEAD)

        #  ставим вторую задачку
        job_2 = self._job_to_queue(pool_name, job_sleep_time)
        #  ждем пока первая выполнится
        self.check_for_timeout(
            callback=lambda: decode_if_not_empty(self.redis.get('test_job {}'.format(job_1.id))),
            check_func=lambda value: value == 'ENDED',  # TODO проверка через базу
        )
        #  проверяем что вторая осталась в очереди
        redis_queue = QueueManager(connection=self.redis)
        queue_job_id = redis_queue.pop(job_2.pool, job_2.id)
        self.assertEqual(queue_job_id, str(job_2.id))
        #  проверяем что пул завершился с нужным статусом
        ret_code = self.pool_process.wait()  # TODO что делать если пул не завершился? ведь будем ждать бесконечно...
        self.assertEqual(ret_code, 0)
        pool_status = self.pool_status_manager.get_status(pool='test_pool', _uuid=_uuid)
        self.assertEqual(pool_status, PoolStatusManager.STATUS_TERMITATED)

    def test_sigterm(self):
        pool_name = 'test_pool'
        job_sleep_time = 1
        # ставим задачку
        job = self._job_to_queue(pool_name, job_sleep_time)
        # запускаем пул
        self.pool_process = self._start_subproc('run_pool', '--name='+pool_name)
        _uuid = self._get_pool_uuid(pool_name=pool_name)
        # тут же его киляем
        self.pool_process.send_signal(signal.SIGTERM)
        # проверяем что задачка таки завершилась
        self.check_for_timeout(
            callback=lambda: decode_if_not_empty(self.redis.get('test_job {}'.format(job.id))),
            check_func=lambda value: value == 'ENDED',
        )
        #  проверяем что пул завершился с нужным статусом
        ret_code = self.pool_process.wait()
        self.assertEqual(ret_code, 0)
        pool_status = self.pool_status_manager.get_status(pool='test_pool', _uuid=_uuid)
        self.assertEqual(pool_status, PoolStatusManager.STATUS_TERMITATED)


