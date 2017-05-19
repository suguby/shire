# -*- coding: utf-8 -*-
import setproctitle

import six
import os
import uuid
import sys

import datetime
import warnings

from shire.const import SHIRE_WORKHORSE_PROCESS_NAME
from shire.exceptions import RestartJobException
from shire.models import JobEntry, db
from shire.utils import capture_output, activate_venv


__all__ = ['Workhorse', 'WorkhorseStatusContext']


class WorkhorseStatusContext(object):

    def __init__(self, workhorse):
        self.workhorse = workhorse

    def __enter__(self):
        job_entry = self.workhorse.job_entry
        job_entry.pool_uuid = self.workhorse.pool.uuid
        job_entry.worker_uuid = self.workhorse.uuid
        job_entry.worker_pid = self.workhorse.pid
        job_entry.status = JobEntry.STATUS_IN_PROGRESS
        job_entry.save(only=[JobEntry.status, JobEntry.worker_uuid, JobEntry.worker_pid, JobEntry.pool_uuid])
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        exceptions_proceeded = False
        job_entry = self.workhorse.job_entry
        if exc_type == RestartJobException:
            if exc_val.wait_minutes:
                # задачка хочет рестартовать отложенно
                job_entry.execute_at = datetime.datetime.now() + datetime.timedelta(minutes=exc_val.wait_minutes)
            job_entry.status = JobEntry.STATUS_RESTART
            exceptions_proceeded = True
        else:
            job_entry.status = JobEntry.STATUS_ENDED
        job_entry.save(only=[JobEntry.status, JobEntry.execute_at])
        return exceptions_proceeded


class Daemon(object):

    def __init__(self):
        self._uuid = None
        self._pid = None

    @property
    def uuid(self):
        if not self._uuid:
            self._uuid = str(uuid.uuid4())
        return self._uuid

    @property
    def pid(self):
        if not self._pid:
            self._pid = os.getpid()
        return self._pid


class Workhorse(Daemon):

    def __init__(self, pool, job_id):
        super(Workhorse, self).__init__()
        self.pool = pool
        self.config = pool.config
        self.log = pool.log
        self.job_id = job_id
        self.job_entry = None

    @property
    def sys_path(self):
        sys_path = self.config.get(self.config.SHIRE_SECTION, {}).get(
            self.config.SHIRE_SYS_PATH, self.config.SHIRE_SYS_PATH_DEFAULT
        )
        if not sys_path:
            return None

        return [os.path.abspath(x) for x in sys_path.split(':')]

    @property
    def venv_path(self):
        venv_path = self.config.get(self.config.SHIRE_SECTION, {}).get(
            self.config.SHIRE_VENV_PATH, self.config.SHIRE_VENV_PATH_DEFAULT
        )
        if not venv_path:
            return None
        return os.path.abspath(venv_path)

    @property
    def venv_exclusive(self):
        venv_exclusive = self.config.get(self.config.SHIRE_SECTION, {}).get(
            self.config.SHIRE_VENV_EXCLUSIVE, self.config.SHIRE_VENV_EXCLUSIVE_DEFAULT
        )
        return venv_exclusive

    def fork(self):
        pid = os.fork()
        if pid:
            return pid
        # унаследовали signal.signal(signal.SIGTERM, pool.terminate)
        # надо сказать инстансу пула, что мы теперь - workhorse
        self.pool.i_am_pool = False
        setproctitle.setproctitle('{} job_id={}'.format(SHIRE_WORKHORSE_PROCESS_NAME, self.job_id) + ' ' * 128)
        # отдельное соединение для потомка
        db.initialize(self.config.get_db())
        try:
            self.run()
        except Exception as exc:
            self.log.exception('In job #{} with workhorse uuid {} (pool {}/{})'.format(
                self.job_id, self.uuid, self.pool.name, self.pool.uuid))
            sys.exit(1)
        else:
            sys.exit(0)
        finally:
            db.close()

    def run(self):
        self.job_entry = self._load_from_db()
        # Активация virtual_env
        job_venv_path = self.job_entry.func_call.get(JobEntry.FUNC_CALL_VENV_PATH, None)
        if job_venv_path is not None:
            job_venv_exclusive = self.job_entry.func_call.get(JobEntry.FUNC_CALL_VENV_EXCLUSIVE, None)
            activate_venv(job_venv_path, job_venv_exclusive)
        elif self.venv_path:
            activate_venv(self.venv_path, self.venv_exclusive)
        # Обновление sys_path
        if self.job_entry.func_call.get(JobEntry.FUNC_CALL_SYS_PATH) is not None:
            sys_path = self.job_entry.func_call[JobEntry.FUNC_CALL_SYS_PATH]
            sys_path = [os.path.abspath(x) for x in sys_path.split(':')]
            for path in reversed(sys_path):
                if path not in sys.path:
                    sys.path.insert(0, path)
        elif self.sys_path:
            for path in reversed(self.sys_path):
                if path not in sys.path:
                    sys.path.insert(0, path)

        stream = six.StringIO()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with capture_output(stream, stream):
                job = self.job_entry.job_cls(workhorse=self)
                args, kwargs = self.job_entry.get_params()
                with WorkhorseStatusContext(workhorse=self):
                    job.run(*args, **kwargs)
        output = stream.getvalue() or None
        if output is not None:
            try:
                self.log.info(u'Got output for job #{}: \n{}'.format(self.job_id, output))
            except UnicodeDecodeError:
                pass

    def _load_from_db(self):
        job_entry = JobEntry.get(JobEntry.id == self.job_id)
        return job_entry
