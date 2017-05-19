# -*- coding: utf-8 -*-


__all__ = ['ShireException', 'PoolInvalidStatusException', 'RestartJobException']


class ShireException(Exception):
    pass


class PoolInvalidStatusException(ShireException):
    pass


class RestartJobException(Exception):

    def __init__(self, wait_minutes=None, *args):
        self.wait_minutes = wait_minutes
        super(RestartJobException, self).__init__(*args)

