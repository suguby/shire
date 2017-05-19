# -*- coding: utf-8 -*-
import os
import tempfile

import shutil

import sys

from shire.pool import Pool
from shire.workhorse import Workhorse
from tests.app.jobs import TestOtherModuleJob, TestVirtualEnvJob
from tests.utils import TestBase


def _create_venv(tmp_dir):
    import six
    if six.PY2:
        import os
        os.system('virtualenv {}'.format(tmp_dir))
    else:
        from venv import EnvBuilder
        builder = EnvBuilder()
        builder.create(tmp_dir)


class TestJobWithSysPath(TestBase):

    def setUp(self):
        self.redis.flushall()
        self.pool = Pool(config=self.config, name='test_pool')
        self.prev_sys_path = list(sys.path)
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        sys.path = self.prev_sys_path
        shutil.rmtree(self.tmp_dir)

    def test_sys_path(self):
        other_module = os.path.join(self.tmp_dir, 'other_module.py')
        with open(other_module, 'w') as ff:
            module_content = [
                'from __future__ import print_function',
                '',
                'def run():',
                '    print("Hello from other module!!!")',
            ]
            ff.write('\n'.join(module_content))
        job = TestOtherModuleJob.delay(
            config=self.config,
            pool='test_pool',
            queue='abc',
            sys_path='{}:/some/dir'.format(self.tmp_dir)
        )
        workhorse = Workhorse(pool=self.pool, job_id=job.id)
        workhorse.run()

        status = self.redis.get('test_job {}'.format(job.id))
        self.assertEquals(status.decode(), 'ENDED')

    def test_venv_path(self):
        _create_venv(self.tmp_dir)

        job = TestVirtualEnvJob.delay(
            config=self.config,
            pool='test_pool',
            queue='abc',
            venv_path=self.tmp_dir,
        )
        workhorse = Workhorse(pool=self.pool, job_id=job.id)
        workhorse.run()
        for item in sys.path:
            if self.tmp_dir in item:
                break
        else:
            raise AssertionError('No our venv in sys.path')

    def test_exclusive_venv_path(self):
        _create_venv(self.tmp_dir)

        job = TestVirtualEnvJob.delay(
            config=self.config,
            pool='test_pool',
            queue='abc',
            venv_path=self.tmp_dir,
            venv_exclusive=True,
        )
        workhorse = Workhorse(pool=self.pool, job_id=job.id)
        workhorse.run()

        site_packages_entries = [item for item in sys.path if 'site-packages' in item]
        for item in site_packages_entries:
            if self.tmp_dir in item:
                break
        else:
            raise AssertionError('No venv in sys.path')

        self.assertEqual(len(site_packages_entries), 1, msg='More then one site-packages in sys.path')



