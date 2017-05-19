# -*- coding: utf-8 -*-

import datetime
import signal
import random

from shire.models import JobEntry
from tests.utils import TestWithPid


class TestHostler(TestWithPid):
    def setUp(self):
        self.config[self.config.HOSTLER_SECTION][self.config.HOSTLER_CHECK_TIME] = '0.5'
        self.config.save(self.config_path)

    @classmethod
    def create_fake_job(cls, **kwargs):
        defaults = {
            'file_path': '/non/existant/path/file.py',
            'file_cls': 'NonExistantJob',
            'pool': 'dummy_pool',
            'queue': 'dummy_queue',
            'host': JobEntry.HOST_DEFAULT,
            'status': JobEntry.STATUS_NEW,
            'args': (),
            'kwargs': {}
        }
        defaults.update(kwargs)
        return JobEntry.create_job(**defaults)

    def test_functional(self):
        hostler_process = self.popen_cli('run_hostler')
        self.active_pid = hostler_process.pid

        # Эмулируем задачу на несуществующем процессе, зависшую 5 часов назад
        job = self.create_fake_job(status=JobEntry.STATUS_IN_PROGRESS)
        job.updated_at = (datetime.datetime.now() - datetime.timedelta(minutes=300))
        while True:
            pid = random.randint(1e3, 65e3)
            if not self.check_pid(pid):
                break
        job.worker_pid = pid
        job.save()

        self.check_for_timeout(
            callback=lambda: JobEntry.get(JobEntry.id == job.id).status == JobEntry.STATUS_RESTART,
            message=u'Задача переустновлена в очередь'
        )

        # Завершаем hostler
        hostler_process.send_signal(signal.SIGTERM)

        # Проверка корректности завершения
        self.check_for_timeout(
            callback=lambda: self.check_pid(self.active_pid), message=u'Hostler завершен некорректно'
        )
