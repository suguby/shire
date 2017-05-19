# -*- coding: utf-8 -*-
import errno
import os
import subprocess
import tempfile
import unittest
import signal
import shutil
import sys
import time

import mockredis

from shire.config import Config
from shire.models import db, JobEntry, Limit, Queue, Crontab

__all__ = ['TestConfig', 'TestBase']


class TestConfig(Config):
    def __init__(self, *args, **kwargs):
        self.__redis = None
        super(TestConfig, self).__init__(*args, **kwargs)

    def get_redis(self):
        if self.__redis is None:
            self.__redis = mockredis.mock_strict_redis_client()
        return self.__redis


class TestBase(unittest.TestCase):
    tmp_dir = None

    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.mkdtemp()
        cls.config = Config()
        cls.config.make_default()
        cls.config[Config.CONNECTION_SECTION][Config.CONNECTION_DB_URL] = 'sqlite:///{}/test.db'.format(cls.tmp_dir)
        cls.config[Config.CONNECTION_SECTION][Config.CONNECTION_REDIS_URL] = 'redis://localhost:6379/15'
        cls.config_path = os.path.join(cls.tmp_dir, 'shire.cfg')
        cls.config.save(cls.config_path)
        cls.redis = cls.config.get_redis()
        db.initialize(cls.config.get_db())
        for model in (JobEntry, Limit, Queue, Crontab):
            model.create_table(True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_dir)

    def check_for_timeout(
        self, callback, check_func=lambda x: x, timeout=5, step=0.1, teardown=lambda: None, message=None
    ):
        current = 0
        while current < timeout:
            val = callback()
            if check_func(val):
                return val
            time.sleep(step)
            current += step
        teardown()
        self.fail("Can't get TRUE value within {} secs".format(timeout) if message is None else message)

    def popen_cli(self, *args):
        popen_start_cmd = [sys.executable, '-m', 'shire.cli', '--config={}'.format(self.config_path)] + list(args)
        return subprocess.Popen(popen_start_cmd)

    @classmethod
    def check_pid(cls, pid):
        try:
            os.kill(pid, signal.SIG_DFL)
        except OSError as e:
            return e.errno == errno.EPERM
        else:
            return True

    def _check_datetime_most_equal(self, dt1, dt2, up_to_minute=True):
        seconds = 60 if up_to_minute else 1
        result = abs((dt1 - dt2).total_seconds()) <= seconds
        self.assertTrue(result)


class TestWithPid(TestBase):
    def setUp(self):
        self.active_pid = 0

    def tearDown(self):
        if self.active_pid > 0 and self.check_pid(self.active_pid):
            os.kill(self.active_pid, signal.SIGKILL)