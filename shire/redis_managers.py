# -*- coding: utf-8 -*-

import uuid

from shire.exceptions import PoolInvalidStatusException

__all__ = ['QueueManager', 'PoolStatusManager', 'LogMessageManager']


class BaseRedisManager(object):

    def __init__(self, connection):
        self.db = connection


class QueueManager(BaseRedisManager):
    PATH = 'shire:to_execute:{pool}'

    def push(self, pool, job_id, to_tail=False):
        key = self.PATH.format(pool=pool)
        if to_tail:
            return self.db.rpush(key, str(job_id))
        return self.db.lpush(key, str(job_id))

    def pop(self, pool, timeout=None):
        key = self.PATH.format(pool=pool)
        res = self.db.brpop(key, timeout=timeout)
        if res:
            key, job_id = res
            return job_id.decode()
        return None

    def show_queue(self, pool):
        return self.db.lrange(self.PATH.format(pool=pool), 0, -1)


class PoolStatusManager(BaseRedisManager):
    BASE_PATH = 'shire:pool'
    POOL_PATH = BASE_PATH + ':{pool}'
    UUID_PATH = POOL_PATH + ':{uuid}'

    STATUS_ACTIVE = 'active'
    STATUS_DEAD = 'dead'
    STATUS_KILL = 'kill'
    STATUS_TERMITATED = 'terminated'
    _valid_statuses = (STATUS_ACTIVE, STATUS_DEAD, STATUS_KILL, STATUS_TERMITATED)

    def get_all(self, pool=None):
        keys = self.POOL_PATH.format(pool=pool) + ':*' if pool else self.BASE_PATH + ':*'
        value = self.db.keys(keys)
        if value:
            return [v.decode() for v in value]
        return value

    def get_status(self, pool, _uuid):
        value = self.db.hget(self.UUID_PATH.format(pool=pool, uuid=_uuid), 'status')
        if value:
            return value.decode()
        return value

    def set_status(self, pool, _uuid, status):
        if status not in self._valid_statuses:
            raise PoolInvalidStatusException(status)
        return self.db.hset(self.UUID_PATH.format(pool=pool, uuid=_uuid), 'status', status)

    def del_status(self, pool, _uuid):
        return self.db.delete(self.UUID_PATH.format(pool=pool, uuid=_uuid))


class LogMessageManager(BaseRedisManager):
    BASE_PATH = 'shire:log:{pool}'
    UUID_PATH = BASE_PATH + ':{uuid}'
    QUEUE_PATH = 'shire:lqueue'
    ID_FORMAT = '{pool}:{uuid}'
    EXPIRE_TIME = 5 * 60

    def write(self, pool, message):
        _uuid = uuid.uuid4()
        self.db.set(self.UUID_PATH.format(pool=pool, uuid=_uuid), message, ex=self.EXPIRE_TIME)
        self.db.lpush(self.QUEUE_PATH, self.ID_FORMAT.format(pool=pool, uuid=_uuid))

    def pop_next(self, timeout=0):
        res = self.db.brpop(self.QUEUE_PATH, timeout=timeout)
        if res:
            queue_name, log = res
            pool, _uuid = log.decode().split(':', 1)
            return {'pool': pool, 'uuid': _uuid}

    def pop(self, pool, _uuid):
        log_path = self.UUID_PATH.format(pool=pool, uuid=_uuid)
        result = self.db.get(log_path)
        if result:
            self.db.delete(log_path)
        return result

