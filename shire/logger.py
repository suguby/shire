# -*- coding: utf-8 -*-

import logging
import sys
import traceback
import json

import six

from shire.redis_managers import LogMessageManager


__all__ = ['RedisLogger']


class RedisLogger(logging.Logger):
    # Специальный логгер shire, не требующий подключения handler'ов

    def __init__(self, config, pool='shire'):
        self.shire_config = config
        self.shire_log_manager = LogMessageManager(config.get_redis())
        super(RedisLogger, self).__init__(pool, level=logging.NOTSET)

    @classmethod
    def format_exception(cls, ei):
        sio = six.StringIO()
        traceback.print_exception(ei[0], ei[1], ei[2], None, sio)
        s = sio.getvalue()
        sio.close()
        if s[-1:] == "\n":
            s = s[:-1]
        return s

    @classmethod
    def format_record(cls, record):
        # Переработанный обработчик из logging.Formatter.format
        record.message = record.getMessage()
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = cls.format_exception(record.exc_info)
        if record.exc_text:
            try:
                '' + record.exc_text
            except UnicodeError:
                record.exc_text = record.exc_text.decode(sys.getfilesystemencoding(), 'replace')
        del record.exc_info
        del record.msg
        return json.dumps(record.__dict__)

    def handle(self, record):
        # Вместо вызова хенделоров сразу пишем в редис
        json_str = self.format_record(record)
        self.shire_log_manager.write(pool=self.name, message=json_str)
