# -*- coding: utf-8 -*-
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import sys

import datetime
from shire.redis_managers import LogMessageManager
from shire.utils import create_console_handler, create_logger


__all__ = ['Scribe']


class Scribe(object):
    POP_TIMEOUT = 1

    # Режимы работы Scribe в зависимости от конфигурации
    MODE_ONE_LOG, MODE_MULTI_LOGS = 0, 1

    SYSTEM_LOG = 'system'
    DEFAULT_LOG = 'default'

    FORMATTER = logging.Formatter('%(message)s')
    OUR_FORMATTER = u'[ %(asctime)s | %(name)s | %(pathname)s:%(lineno)d ] - <%(levelname)s> - %(message)s'

    EXPIRED_NOTIFY_LIMIT = 500

    def __init__(self, config, verbose=False):
        self.config = config
        self.verbose = verbose
        self.scribe_logger = create_logger('shire.scribe_proccess')
        if verbose:
            create_console_handler(self.scribe_logger)
        self.section = self.config.section_getter(self.config.SCRIBE_SECTION)
        self.manager = LogMessageManager(self.config.get_redis())
        self.log_dir = self.section.get(self.config.SCRIBE_DIRECTORY, self.config.SCRIBE_DIRECTORY_DEFAULT)
        self.max_logs = int(self.section.get(self.config.SCRIBE_MAX_LOGS, self.config.SCRIBE_MAX_LOGS_DEFAULT))
        self.log_size = int(self.section.get(self.config.SCRIBE_MAX_SIZE, self.config.SCRIBE_MAX_SIZE_DEFAULT))
        self.handlers = {}
        self.loggers = {}
        self._expired_logs_count = 0
        self.setup_handlers()
        self.log_start()

    def scribe_log(self, msg, level='info'):
        if not self.verbose:
            return
        getattr(self.scribe_logger, level)(msg)

    @property
    def expired_logs_count(self):
        return self._expired_logs_count

    @expired_logs_count.setter
    def expired_logs_count(self, value):
        self._expired_logs_count = value
        if value % self.EXPIRED_NOTIFY_LIMIT == 1:
            self.notify_expire_count()

    def get_full_path(self, filename):
        return os.path.join(self.log_dir, filename)

    @property
    def mode(self):
        if self.section.get(self.config.SCRIBE_PER_POOL, self.config.SCRIBE_PER_POOL_DEFAULT) != '1':
            return self.MODE_ONE_LOG
        return self.MODE_MULTI_LOGS

    @property
    def system(self):
        return self.loggers[self.SYSTEM_LOG]

    def make_logger(self, name):
        logger = logging.getLogger('shire.scribe.{}'.format(name))
        logger.setLevel(logging.DEBUG)
        self.loggers[name] = logger
        return logger

    def make_handler(self, name, path):
        handler = RotatingFileHandler(path, maxBytes=self.log_size, backupCount=self.max_logs)
        handler.setFormatter(self.FORMATTER)
        handler.setLevel(logging.DEBUG)
        self.handlers[name] = handler
        return handler

    def make_logger_and_handler(self, name, path):
        logger = self.make_logger(name)
        handler = self.make_handler(name, path)
        logger.addHandler(handler)

    def get_pool_logger(self, name):
        if self.mode == self.MODE_ONE_LOG:
            return self.loggers[self.DEFAULT_LOG]
        logger_name = 'pool.{}'.format(name)
        if logger_name not in self.loggers:
            self.make_logger_and_handler(logger_name, self.get_full_path('{}.log'.format(name)))
        return self.loggers[logger_name]

    def setup_handlers(self):
        self.make_logger_and_handler(self.SYSTEM_LOG, self.get_full_path(
            self.section.get(self.config.SCRIBE_SYSTEM_LOG, self.config.SCRIBE_SYSTEM_LOG_DEFAULT)
        ))
        if self.mode == self.MODE_ONE_LOG:
            self.make_logger_and_handler(self.DEFAULT_LOG, self.get_full_path(
                self.section.get(self.config.SCRIBE_DEFAULT_LOG, self.config.SCRIBE_DEFAULT_LOG_DEFAULT)
            ))

    def log_start(self):
        self.system.info('Scribe process started')

    def notify_expire_count(self):
        self.loggers[self.SYSTEM_LOG].info('Expired logs count: {}'.format(self.expired_logs_count))

    def write_log(self, log):
        log_data = self.manager.pop(log['pool'], log['uuid'])
        if not log_data:
            self.expired_logs_count += 1
            self.scribe_log('Got expired log for pool {}'.format(log['pool']))
            return
        log_data = json.loads(log_data)
        log_data['asctime'] = datetime.datetime.fromtimestamp(log_data['created'])
        # Форматируем оригинальное сообщение лога
        message = self.OUR_FORMATTER % log_data
        if 'exc_text' in log_data:
            message = u'{}\n{}'.format(message, log_data['exc_text'])
        logger = self.get_pool_logger(name=log['pool'])
        level = logging._checkLevel(log_data['levelname'])
        logger.log(level, message)

    def run(self):
        self.scribe_log('Scribe started')
        self.scribe_log(
            'Scribe mode: {}'.format(
                {self.MODE_MULTI_LOGS: 'multi logs', self.MODE_ONE_LOG: 'single log'}.get(self.mode)
            )
        )
        self.scribe_log('Logs directory: {}'.format(self.log_dir))
        try:
            while True:
                log = self.manager.pop_next(self.POP_TIMEOUT)
                if log:
                    self.write_log(log)
        except Exception as e:
            self.system.exception(e)
            self.system.error('Scribe process terminated')
            self.scribe_log(e, 'exception')
            sys.exit(1)
