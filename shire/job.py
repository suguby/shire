# -*- coding: utf-8 -*-
import datetime
import sys
import time

from shire.exceptions import RestartJobException
from shire.models import JobEntry, Crontab
from shire.pool import Pool
from shire.workhorse import Workhorse

__all__ = ['Job', 'RestartJobException']


class DummyWorkhorse(Workhorse):
    def __init__(self, config):
        pool = Pool(config, 'dummy_pool')
        super(DummyWorkhorse, self).__init__(pool=pool, job_id=None)


def start_job(config, **kwargs):
    with config.with_db():
        job_entry = JobEntry.create_job(**kwargs)
        job_entry.save()
        return job_entry


class Job(object):

    @classmethod
    def delay(cls, config, pool, queue=None, host=None, args=(), kwargs=None,
              sys_path=None, venv_path=None, venv_exclusive=None, wait_minutes=0):
        kwargs = {} if kwargs is None else kwargs
        queue = pool if queue is None else queue
        job_entry = start_job(
            config, pool=pool, queue=queue, host=host,
            file_path=sys.modules[cls.__module__].__file__, file_cls=cls.__name__,
            args=args, kwargs=kwargs,
            sys_path=sys_path, venv_path=venv_path, venv_exclusive=venv_exclusive, wait_minutes=wait_minutes,
        )
        return job_entry

    @classmethod
    def execute(cls, config, args=(), kwargs=None, job_entry=None):
        # Немедленный вызов задачи на выполнение, минуя whip
        kwargs = {} if kwargs is None else kwargs
        # TODO здесь нужны sys_path и venv_path?
        job_entry = start_job(
            config, pool='dummy_pool', queue='dummy_queue', file_path=sys.modules[cls.__module__].__file__,
            file_cls=cls.__name__, args=args, kwargs=kwargs, status=JobEntry.STATUS_ENQUEUED, host='dummy_host'
        ) if job_entry is None else job_entry
        # Т.к. execute однопоточный, то повторять выполнение мы должны прямо здесь.
        # Чтобы не достигнуть maximum recursion depth exceeded раскрываем в while
        while True:
            try:
                job_entry.job_cls(DummyWorkhorse(config)).run(*args, **kwargs)
            except RestartJobException as exc:
                if exc.wait_minutes:
                    # задачка хочет рестартовать отложенно
                    time.sleep(exc.wait_minutes * 60)
                continue
            else:
                break
        return job_entry

    def __init__(self, workhorse):
        self.workhorse = workhorse

    @property
    def log(self):
        return self.workhorse.log

    @property
    def config(self):
        return self.workhorse.config

    @property
    def job_entry(self):
        return self.workhorse.job_entry

    def tick(self):
        # Обновляет информацию о последнем обновлении задачи в базу данных
        self.job_entry.updated_at = datetime.datetime.now()
        self.job_entry.save(only=[JobEntry.updated_at])

    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def restart(self, wait_minutes=None):
        raise RestartJobException(wait_minutes=wait_minutes)

    @classmethod
    def schedule(cls, config, key, cron_string, pool, queue=None, host=None, args=(), kwargs=None,
                 sys_path=None, venv_path=None):
        kwargs = {} if kwargs is None else kwargs
        queue = pool if queue is None else queue
        host = JobEntry.HOST_DEFAULT if host is None else host
        with config.with_db():
            func_call = JobEntry.make_func_call(
                file_path=sys.modules[cls.__module__].__file__,
                file_cls=cls.__name__,
                args=list(args), kwargs=kwargs, sys_path=sys_path, venv_path=venv_path
            )
            crontab = Crontab.select().where(Crontab.key == key).first()
            edited = False
            if crontab is None:
                crontab = Crontab(key=key, cron_string=cron_string, func_call=func_call)
                edited = True
            for key, value in dict(cron_string=cron_string, pool=pool, queue=queue,
                                   host=host, func_call=func_call).items():
                if getattr(crontab, key) != value:
                    setattr(crontab, key, value)
                    edited = True
            if edited:
                # Изменились параметры cron_string, значит надо заново настроить запуски
                crontab.last_scheduled = None
            crontab.save()
