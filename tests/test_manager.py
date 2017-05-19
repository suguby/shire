# -*- coding: utf-8 -*-

import uuid
from unittest import TestCase

from shire.redis_managers import PoolStatusManager
from shire.manager import ShireManager

from tests.utils import TestConfig


class TestShireManager(TestCase):
    FIRST_TEST_POOL = 'test1'
    SECOND_TEST_POOL = 'test2'

    def setUp(self):
        self.config = TestConfig()
        self.config.make_default()
        self.redis = self.config.get_redis()
        self.redis.flushdb()
        self.manager = ShireManager(self.config)
        self.pool_manager = PoolStatusManager(self.redis)

    def emulate_pools(self, status=PoolStatusManager.STATUS_ACTIVE):
        # Эмулируем пулы в нужном статусе
        uuid1, uuid2 = uuid.uuid4(), uuid.uuid4()
        self.pool_manager.set_status(self.FIRST_TEST_POOL, uuid1, status)
        self.pool_manager.set_status(self.SECOND_TEST_POOL, uuid2, status)
        return str(uuid1), str(uuid2)

    def pool_status(self, pool, _uuid):
        return self.pool_manager.get_status(pool, _uuid)

    def test_terminate_once(self):
        uuid1, uuid2 = self.emulate_pools()
        self.manager.terminate_pools(pools=(self.FIRST_TEST_POOL,))
        self.assertEqual(
            self.pool_status(self.FIRST_TEST_POOL, uuid1),
            PoolStatusManager.STATUS_DEAD,
            u'Первый пул в статусе DEAD'
        )
        self.assertEqual(
            self.pool_status(self.SECOND_TEST_POOL, uuid2),
            PoolStatusManager.STATUS_ACTIVE,
            u'Второй пул работает как обычно'
        )

    def test_terminate(self):
        uuid1, uuid2 = self.emulate_pools()
        self.manager.terminate_pools()
        self.assertEqual(
            (self.pool_status(self.FIRST_TEST_POOL, uuid1), self.pool_status(self.SECOND_TEST_POOL, uuid2),),
            (PoolStatusManager.STATUS_DEAD, PoolStatusManager.STATUS_DEAD,),
            u'Оба пула в статусе DEAD'
        )

    def test_clear_by_status(self):
        self.emulate_pools()
        self.manager.clean_pools(from_statuses=(PoolStatusManager.STATUS_ACTIVE,))
        self.assertEqual(self.pool_manager.get_all(), [], u'Пулы вычищены из redis')

    def test_clear(self):
        self.emulate_pools(PoolStatusManager.STATUS_TERMITATED)
        self.manager.clean_pools()
        pools = self.pool_manager.get_all()
        self.assertEqual(pools, [], u'Пулы вычищены из redis')

    def test_kill_not_dead(self):
        # По умолчанию не должны убиваться пулы, которые не имеют статуса DEAD
        uuid1, uuid2 = self.emulate_pools()
        self.manager.kill_pools(pools=(self.FIRST_TEST_POOL,))
        self.assertEqual(
            self.pool_status(self.FIRST_TEST_POOL, uuid1),
            PoolStatusManager.STATUS_ACTIVE,
            u'Первый пул все ещё активен'
        )

    def test_kill(self):
        uuid1, uuid2 = self.emulate_pools(PoolStatusManager.STATUS_DEAD)
        self.manager.kill_pools()
        self.assertEqual(
            (self.pool_status(self.FIRST_TEST_POOL, uuid1), self.pool_status(self.SECOND_TEST_POOL, uuid2),),
            (PoolStatusManager.STATUS_KILL, PoolStatusManager.STATUS_KILL,),
            u'Оба пула в статусе KILL'
        )

    def test_status(self):
        uuid1, uuid2 = self.emulate_pools()
        self.pool_manager.set_status(self.SECOND_TEST_POOL, uuid2, PoolStatusManager.STATUS_DEAD)
        by_pool = {x['name']: x for x in self.manager.get_status()}
        self.assertEqual(
            by_pool[self.FIRST_TEST_POOL],
            {'status': PoolStatusManager.STATUS_ACTIVE, 'name': self.FIRST_TEST_POOL, 'uuid': uuid1},
            u'Информация по первому пулу верна'
        )
        self.assertEqual(
            by_pool[self.SECOND_TEST_POOL],
            {'status': PoolStatusManager.STATUS_DEAD, 'name': self.SECOND_TEST_POOL, 'uuid': uuid2},
            u'Информация по второму пулу верна'
        )
