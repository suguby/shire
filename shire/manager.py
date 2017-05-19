# -*- coding: utf-8 -*-

from shire.redis_managers import PoolStatusManager


__all__ = ['ShireManager']


class ShireManager(object):
    CONST_ALL = '__all__'
    EXPORT_METHODS = (
        'get_status', 'terminate_pools', 'kill_pools', 'clean_pools'
    )

    def __init__(self, config):
        self.config = config
        self.status_manager = PoolStatusManager(self.config.get_redis())

    def _prepare_statuses(self, statuses):
        if statuses == self.CONST_ALL:
            statuses = (
                PoolStatusManager.STATUS_DEAD, PoolStatusManager.STATUS_KILL,
                PoolStatusManager.STATUS_ACTIVE, PoolStatusManager.STATUS_TERMITATED
            )
        return set(statuses)

    def get_pools(self):
        # TODO: Добавить опциональный параметр get_extra=False, при получении которого загружать так же из
        # базы данных кол-во активных, ожидающих и выполненных задач, а так же время последнего обновления задачи
        for key in self.status_manager.get_all():
            try:
                prefix, pool, uuid = key.rsplit(':', 2)
            except ValueError:
                # Плохое значение в get_all. Кейс скорее невозможный, проверка на всякий
                continue
            yield {
                'name': pool,
                'uuid': uuid,
                'status': self.status_manager.get_status(pool, uuid)
            }

    def get_status(self, pools=CONST_ALL, from_statuses=CONST_ALL):
        statuses = self._prepare_statuses(from_statuses)
        check_name = (lambda x: True) if pools == self.CONST_ALL else (lambda x: x in pools)
        for pool in self.get_pools():
            if pool['status'] in statuses and check_name(pool['name']):
                yield pool

    def update_pool_statuses(self, status, pools=CONST_ALL, from_statuses=CONST_ALL):
        statuses = self._prepare_statuses(from_statuses)
        check_name = (lambda x: True) if pools == self.CONST_ALL else (lambda x: x in pools)
        for pool in self.get_pools():
            if pool['status'] in statuses and check_name(pool['name']):
                self.status_manager.set_status(pool['name'], pool['uuid'], status)

    def terminate_pools(self, pools=CONST_ALL, from_statuses=(PoolStatusManager.STATUS_ACTIVE,)):
        # Останавливаем пулы, посылая им статус DEAD
        self.update_pool_statuses(
            PoolStatusManager.STATUS_DEAD, pools=pools, from_statuses=from_statuses
        )

    def kill_pools(self, pools=CONST_ALL, from_statuses=(PoolStatusManager.STATUS_DEAD,)):
        # Приказывает пулам принудительно уничтожить всех своих потомков, посылая им статус KILL
        self.update_pool_statuses(
            PoolStatusManager.STATUS_KILL, pools=pools, from_statuses=from_statuses
        )

    def clean_pools(self, from_statuses=(PoolStatusManager.STATUS_TERMITATED,), **kwargs):
        # Очищает записи в redis от устаревших uuid
        statuses = self._prepare_statuses(from_statuses)
        for pool in self.get_pools():
            if pool['status'] in statuses:
                self.status_manager.del_status(pool['name'], pool['uuid'])

    def run_command(self, command, *args, **kwargs):
        if command in self.EXPORT_METHODS:
            return getattr(self, command)(*args, **kwargs)
