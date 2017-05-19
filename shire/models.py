# -*- coding: utf-8 -*-
import datetime
import json
import sys
import os
import peewee

if sys.version_info < (3, 5):
    # Python 2 и старые версии 3
    import imp

    def import_from_source(fake_name, path):
        return imp.load_source(fake_name, path)
else:
    import importlib.util

    def import_from_source(fake_name, path):
        spec = importlib.util.spec_from_file_location(fake_name, path)
        _module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_module)
        sys.modules[fake_name] = _module
        return _module


__all__ = ['db', 'Limit', 'JobEntry', 'Queue']


db = peewee.Proxy()


class BaseModel(peewee.Model):
    class Meta:
        database = db


class Limit(BaseModel):
    ENTITY_POOL = 'pool'
    ENTITY_QUEUE = 'queue'
    ENTITY_CHOICES = ((ENTITY_POOL, u'Пул'), (ENTITY_QUEUE, u'Очередь'))

    entity_type = peewee.CharField(max_length=16, choices=ENTITY_CHOICES)
    entity = peewee.CharField(max_length=256)
    limit = peewee.IntegerField(default=1)

    class Meta:
        db_table = 'shire_limit'


class Queue(BaseModel):
    # TODO проверить использование - очереди у нас виртуальные и живут в неймспейсе пула, лимиты на них в Limit
    name = peewee.CharField(max_length=256)
    pool = peewee.CharField(max_length=128)

    class Meta:
        db_table = 'shire_queue'


class JobEntry(BaseModel):
    STATUS_NEW = 'new'
    STATUS_RESTART = 'restart'
    STATUS_ENQUEUED = 'enqueued'
    STATUS_IN_PROGRESS = 'in_progress'
    STATUS_ENDED = 'ended'
    STATUS_CHOICES = (
        (STATUS_NEW, u'новая'),
        (STATUS_RESTART, u'на перезапуск'),
        (STATUS_ENQUEUED, u'в очереди'),
        (STATUS_IN_PROGRESS, u'выполняется'),
        (STATUS_ENDED, u'завершена'),
    )
    STATUS_DEFAULT = STATUS_NEW
    VALID_STATUSES = {k for k, v in STATUS_CHOICES}
    FUNC_CALL_PATH = 'path'
    FUNC_CALL_CLASS = 'class'
    FUNC_CALL_ARGS = 'args'
    FUNC_CALL_KWARGS = 'kwargs'
    FUNC_CALL_SYS_PATH = 'sys_path'
    FUNC_CALL_VENV_PATH = 'venv_path'
    FUNC_CALL_VENV_EXCLUSIVE = 'venv_exclusive'
    HOST_DEFAULT = 'default'

    func_call_ = peewee.TextField(db_column='func_call')
    pool = peewee.CharField(max_length=256, index=True)
    queue = peewee.CharField(max_length=256)
    status = peewee.CharField(max_length=32, choices=STATUS_CHOICES, default=STATUS_DEFAULT, index=True)
    host = peewee.CharField(max_length=256, default=HOST_DEFAULT, index=True)
    pool_uuid = peewee.CharField(max_length=64, null=True, index=True)
    worker_uuid = peewee.CharField(max_length=64, null=True)
    worker_pid = peewee.IntegerField(default=0, null=True)
    created_at = peewee.DateTimeField(default=datetime.datetime.now)
    execute_at = peewee.DateTimeField(default=datetime.datetime.now, index=True)
    updated_at = peewee.DateTimeField(default=None, null=True, index=True)
    ended_at = peewee.DateTimeField(default=None, null=True)

    class Meta:
        db_table = 'shire_job'

    @property
    def func_call(self):
        return json.loads(self.func_call_)

    @func_call.setter
    def func_call(self, value):
        self.func_call_ = json.dumps(value)

    @classmethod
    def create_job(cls, file_path, file_cls, pool, queue=None, status=None, host=None, args=None, kwargs=None,
                   sys_path=None, venv_path=None, venv_exclusive=None, wait_minutes=0):
        status = status if status in cls.VALID_STATUSES else cls.STATUS_DEFAULT
        host = cls.HOST_DEFAULT if host is None else host
        job = cls(pool=pool, queue=queue, status=status, host=host)
        job.func_call = cls.make_func_call(
            file_path=file_path, file_cls=file_cls, args=args, kwargs=kwargs,
            sys_path=sys_path, venv_path=venv_path, venv_exclusive=venv_exclusive)
        if wait_minutes > 0:
            job.execute_at = datetime.datetime.now() + datetime.timedelta(minutes=wait_minutes)
        return job

    @classmethod
    def make_func_call(cls, file_path, file_cls, args=None, kwargs=None,
                       sys_path=None, venv_path=None, venv_exclusive=None):
        func_call = {
            cls.FUNC_CALL_PATH: file_path,
            cls.FUNC_CALL_CLASS: file_cls,
            cls.FUNC_CALL_ARGS: () if args is None else args,
            cls.FUNC_CALL_KWARGS: {} if kwargs is None else kwargs
        }
        if sys_path is not None:
            func_call[cls.FUNC_CALL_SYS_PATH] = sys_path
        if venv_path is not None:
            func_call[cls.FUNC_CALL_VENV_PATH] = venv_path
            if venv_exclusive is not None:
                func_call[cls.FUNC_CALL_VENV_EXCLUSIVE] = venv_exclusive
        return func_call

    @property
    def job_cls(self):
        module_path = self.func_call[self.FUNC_CALL_PATH]
        if module_path.endswith('.pyc'):
            module_path = module_path[:-1]
        module_name = 'shire_fake_module{}'.format(module_path.rsplit('.', 1)[0].replace(os.path.sep, '_'))
        fake_module = import_from_source(module_name, module_path)
        return getattr(fake_module, self.func_call[self.FUNC_CALL_CLASS])

    def get_params(self):
        # параметры для конкретной задачи
        args, kwargs = [], {}
        if self.FUNC_CALL_ARGS in self.func_call:
            args = self.func_call[self.FUNC_CALL_ARGS]
            if not isinstance(args, list):
                raise Exception('func_call args {} must be list!'.format(args))
        if self.FUNC_CALL_KWARGS in self.func_call:
            kwargs = self.func_call[self.FUNC_CALL_KWARGS]
            if not isinstance(kwargs, dict):
                raise Exception('func_call kwargs {} must be dict!'.format(kwargs))
        return args, kwargs

    def set_status(self, status):
        self.status = status
        self.save(only=[JobEntry.status])

    def save(self, force_insert=False, only=None):
        if 'updated_at' not in self._dirty:
            if only is not None and self.__class__.updated_at not in only:
                only = [x for x in only] + [self.__class__.updated_at]
            self.updated_at = datetime.datetime.now()
        return super(self.__class__, self).save(force_insert=force_insert, only=only)


class Crontab(BaseModel):
    key = peewee.CharField(max_length=250, index=True)
    cron_string = peewee.TextField()
    description = peewee.TextField(null=True)
    last_scheduled = peewee.DateTimeField(null=True)

    func_call_ = peewee.TextField(db_column='func_call')
    pool = peewee.CharField(max_length=256, index=True)
    queue = peewee.CharField(max_length=256)
    host = peewee.CharField(max_length=256, default=JobEntry.HOST_DEFAULT, index=True)

    class Meta:
        db_table = 'shire_crontab'

    @property
    def func_call(self):
        return json.loads(self.func_call_)

    @func_call.setter
    def func_call(self, value):
        self.func_call_ = json.dumps(value)
