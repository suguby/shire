# -*- coding: utf-8 -*-

import json
import os
from unittest import TestCase

from shire.logger import RedisLogger
from shire.redis_managers import LogMessageManager

from tests.utils import TestConfig


class TestRedisLogger(TestCase):
    TEST_POOL = 'test'

    def setUp(self):
        self.config = TestConfig()
        self.config.make_default()
        # path = os.path.dirname(__file__)
        # config_path = os.path.join(os.path.dirname(path), 'shire.cfg')
        # from shire.config import Config
        # self.config = Config()
        # self.config.load(config_path)
        self.logger = RedisLogger(self.config, self.TEST_POOL)
        self.redis = self.config.get_redis()

    # def tearDown(self):
    #     self.redis.flushall()

    def get_messages_in_pool(self, pool=TEST_POOL):
        return self.redis.keys(LogMessageManager.BASE_PATH.format(pool=pool) + ':*')

    def get_messages_in_queue_len(self, pool=TEST_POOL):
        return self.redis.llen(LogMessageManager.QUEUE_PATH.format(pool=pool))

    def get_last_queue_message(self, pool=TEST_POOL):
        log = self.redis.lindex(LogMessageManager.QUEUE_PATH.format(pool=pool), -1)
        pool, _uuid = log.decode().split(':', 1)
        return {'pool': pool, 'uuid': _uuid}

    def load_message(self, key):
        value = self.redis.get(key)
        return json.loads(value.decode())

    def message_uuid(self, key):
        return key.split(':')[-1]

    def get_first_message(self, pool=TEST_POOL):
        message_key = LogMessageManager.UUID_PATH.format(pool=pool, uuid=self.get_last_queue_message()['uuid'])
        message = self.load_message(message_key)
        return message_key, message

    def assert_redis_is_clear(self):
        self.assertEqual(len(self.get_messages_in_pool()), 0, u'Редис чистый')
        self.assertEqual(self.get_messages_in_queue_len(), 0, u'Очередь чистая')

    def assert_redis_has_n_messages(self, num):
        self.assertEqual(
            len(self.get_messages_in_pool()), num, u'Количество сообщений с информацией в редисе {}'.format(num)
        )
        self.assertEqual(self.get_messages_in_queue_len(), num, u'Количество uuid в очереди редиса {}'.format(num))

    def test_info(self):
        self.assert_redis_is_clear()
        self.logger.info('test')
        self.assert_redis_has_n_messages(1)

        message_key, message = self.get_first_message()

        self.assertEqual(message['levelno'], 20, u'Уровень важности лога - INFO')
        self.assertEqual(message['message'], 'test', u'Сообщения лога корректно')
        self.assertEqual(message['exc_text'], None, u'Ошибки нет')

        message_uuid = self.message_uuid(message_key)
        self.assertEqual(
            self.get_last_queue_message()['uuid'], message_uuid, u'Первый лог на обработку в очереди корректный'
        )

    def test_exception(self):
        self.assert_redis_is_clear()
        try:
            0 / 0
        except Exception as e:
            self.logger.exception(e)
        self.logger.error('error')
        self.assert_redis_has_n_messages(2)

        message_key, message = self.get_first_message()
        self.assertIsNotNone(message['exc_text'], u'Присутствует сообщение об ошибке')
        self.assertEqual(message['levelno'], 40, u'Уровень важности лога - ERROR')
        self.assertIn(
            'by zero', message['message'], u'Сгенерировано корректное сообщение об ошибке'
        )

        message_uuid = self.message_uuid(message_key)
        self.assertEqual(
            self.get_last_queue_message()['uuid'], message_uuid, u'Первый лог на обработку в очереди корректный'
        )
