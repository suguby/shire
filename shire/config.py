# -*- coding: utf-8 -*-
import codecs

import six
from six.moves import configparser

from playhouse.db_url import connect
from redis import StrictRedis


__all__ = ['Config']


class Config(dict):
    SHIRE_SECTION = 'shire'
    SHIRE_HOST = 'host'
    SHIRE_HOST_DEFAULT = 'default'
    SHIRE_SYS_PATH = 'sys_path'
    SHIRE_SYS_PATH_DEFAULT = ''
    SHIRE_VENV_PATH = 'venv_path'
    SHIRE_VENV_PATH_DEFAULT = ''
    SHIRE_VENV_EXCLUSIVE = 'venv_path'
    SHIRE_VENV_EXCLUSIVE_DEFAULT = None

    CONNECTION_SECTION = 'connection'
    CONNECTION_DB_URL = 'db_url'
    CONNECTION_DB_URL_DEFAULT = 'sqlite:///shire.db'
    CONNECTION_REDIS_URL = 'redis_url'
    CONNECTION_REDIS_URL_DEFAULT = 'redis://localhost:6379/0'

    POOL_SECTION = 'pool'
    POOL_CHECK_TIME = 'check_time'
    POOL_CHECK_TIME_DEFAULT = '30'
    
    SCRIBE_SECTION = 'scribe'
    SCRIBE_PER_POOL = 'per_pool'
    SCRIBE_PER_POOL_DEFAULT = '1'
    SCRIBE_DIRECTORY = 'log_directory'
    SCRIBE_DIRECTORY_DEFAULT = 'logs'
    SCRIBE_SYSTEM_LOG = 'system_log'
    SCRIBE_SYSTEM_LOG_DEFAULT = 'shire.log'
    SCRIBE_DEFAULT_LOG = 'default_log'
    SCRIBE_DEFAULT_LOG_DEFAULT = 'default.log'
    SCRIBE_MAX_SIZE = 'log_max_size'
    SCRIBE_MAX_SIZE_DEFAULT = str(100 * 1024 * 1024)  # 100 мегабайт
    SCRIBE_MAX_LOGS = 'max_logs'
    SCRIBE_MAX_LOGS_DEFAULT = '5'

    HOSTLER_SECTION = 'hostler'
    HOSTLER_CHECK_TIME = 'check_time'
    HOSTLER_CHECK_TIME_DEFAULT = '5'

    WHIP_SECTION = 'whip'
    WHIP_CHECK_TIME = 'check_time'
    WHIP_CHECK_TIME_DEFAULT = '1'
    WHIP_LIMITS_UPDATE_TIME = 'limits_update_time'
    WHIP_LIMITS_UPDATE_TIME_DEFAULT = '60'
    WHIP_MAX_JOBS = 'max_jobs'
    WHIP_MAX_JOBS_DEFAULT = '100'

    def __init__(self, *args, **kwargs):
        self.cp = configparser.ConfigParser()
        super(Config, self).__init__(*args, **kwargs)

    def make_default(self):
        self.clear()
        self.update({
            self.SHIRE_SECTION: {
                self.SHIRE_HOST: self.SHIRE_HOST_DEFAULT,
                self.SHIRE_SYS_PATH: self.SHIRE_SYS_PATH_DEFAULT,
                self.SHIRE_VENV_PATH: self.SHIRE_VENV_PATH_DEFAULT
            },
            self.CONNECTION_SECTION: {
                self.CONNECTION_DB_URL: self.CONNECTION_DB_URL_DEFAULT,
                self.CONNECTION_REDIS_URL: self.CONNECTION_REDIS_URL_DEFAULT
            },
            self.POOL_SECTION: {
                self.POOL_CHECK_TIME: self.POOL_CHECK_TIME_DEFAULT
            },
            self.SCRIBE_SECTION: {
                self.SCRIBE_PER_POOL: self.SCRIBE_PER_POOL_DEFAULT,
                self.SCRIBE_DIRECTORY: self.SCRIBE_DIRECTORY_DEFAULT,
                self.SCRIBE_DEFAULT_LOG: self.SCRIBE_DEFAULT_LOG_DEFAULT,
                self.SCRIBE_MAX_SIZE: self.SCRIBE_MAX_SIZE_DEFAULT,
                self.SCRIBE_MAX_LOGS: self.SCRIBE_MAX_LOGS_DEFAULT
            },
            self.HOSTLER_SECTION: {
                self.HOSTLER_CHECK_TIME: self.HOSTLER_CHECK_TIME_DEFAULT
            },
            self.WHIP_SECTION: {
                self.WHIP_CHECK_TIME: self.WHIP_CHECK_TIME_DEFAULT,
                self.WHIP_LIMITS_UPDATE_TIME: self.WHIP_LIMITS_UPDATE_TIME_DEFAULT
            }
        })
        self.save_to_cp()

    def load(self, file_path):
        self.cp = configparser.ConfigParser()
        self.cp.read(file_path)
        self.load_from_cp()

    def load_from_cp(self):
        for section in self.cp.sections():
            for option in self.cp.options(section):
                value = self.cp.get(section, option)
                self.setdefault(section, {})[option] = value

    def save(self, file_path):
        self.save_to_cp()
        self.cp.write(codecs.open(file_path, 'w', 'utf-8'))

    def save_to_cp(self):
        self.cp = configparser.ConfigParser()
        for section, variables in self.items():
            self.cp.add_section(section)
            for name, value in variables.items():
                self.cp.set(section, name, value)

    def get_db(self):
        url = self.get(self.CONNECTION_SECTION, {}).get(self.CONNECTION_DB_URL, None)
        if url is None:
            return None
        return connect(url)

    class _with_db(object):
        def __init__(self, database_getter):
            from shire.models import db
            try:
                is_closed = db.is_closed()
            except AttributeError:
                # Это неинициализированный Proxy
                is_closed = True
            self.need_manage = is_closed
            self.db = db
            if is_closed:
                db.initialize(database_getter())

        def __enter__(self):
            if self.need_manage:
                self.db.connect()

        def __exit__(self, exc_type, exc_val, exc_tb):
            if self.need_manage:
                self.db.close()

    def with_db(self):
        return self._with_db(self.get_db)

    def get_redis(self):
        url = self.get(self.CONNECTION_SECTION, {}).get(self.CONNECTION_REDIS_URL, None)
        if url is None:
            return None
        return StrictRedis.from_url(url)

    def from_section(self, section, key, default=None):
        return self.get(section, {}).get(key, default)

    def section_getter(self, section):
        return type('_', (object,), {'get': lambda s, key, default=None: self.from_section(section, key, default)})()

    def to_string_io(self):
        string = six.StringIO()
        self.save_to_cp()
        self.cp.write(string)
        return string
