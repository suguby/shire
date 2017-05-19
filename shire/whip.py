# -*- coding: utf-8 -*-

import collections
import time

import datetime

from shire.models import db, JobEntry, Limit, Queue
from shire.redis_managers import QueueManager
from shire.utils import create_console_handler, create_logger


__all__ = ['Whip']


class Whip(object):
    # Специально написан по методологии "let it crash".
    # В случае непредвиденных ситуаций должен быть перезапущен супервайзером, а не пытаться разрешить их самостоятельно,
    # в ущерб стабильности

    def __init__(self, config, verbose=False):
        self.config = config
        self.verbose = verbose
        self.whip_logger = create_logger('shire.whip')
        if verbose:
            create_console_handler(self.whip_logger)
        self.section = self.config.section_getter(self.config.WHIP_SECTION)
        db.initialize(self.config.get_db())
        self.queue_limits = {}
        self.pool_limits = {}
        self.queue_to_pool = {}
        self.max_jobs_limit = int(self.section.get(self.config.WHIP_MAX_JOBS, self.config.WHIP_MAX_JOBS_DEFAULT))
        self.redis_queue = QueueManager(self.config.get_redis())
        self.host = self.section.get(self.config.SHIRE_HOST, self.config.SHIRE_HOST_DEFAULT)

    def whip_log(self, msg, level='info'):
        if not self.verbose:
            return
        getattr(self.whip_logger, level)(msg)

    def update_limits(self):
        self.queue_to_pool = {x.name: x.pool for x in Queue.select()}
        for limit in Limit.select():
            if limit.entity_type == Limit.ENTITY_QUEUE:
                self.queue_limits[limit.entity] = limit.limit
            elif limit.entity_type == Limit.ENTITY_POOL:
                self.pool_limits[limit.entity] = limit.limit
        self.whip_log('Limits updated')

    def load_current_jobs(self):
        total = 0
        by_pool = collections.defaultdict(int)
        by_queue = collections.defaultdict(int)

        for job_entry in JobEntry.select(
                JobEntry.id, JobEntry.pool, JobEntry.queue
        ).where(JobEntry.status << [JobEntry.STATUS_IN_PROGRESS, JobEntry.STATUS_ENQUEUED]):
            total += 1
            by_pool[job_entry.pool] += 1
            by_queue[job_entry.queue] += 1

        return {
            'total': total,
            'by_pool': by_pool,
            'by_queue': by_queue
        }

    def enqueue_job(self, job_entry):
        self.whip_log('Job #{} enqueued (Pool: {})'.format(job_entry.id, job_entry.pool))
        self.redis_queue.push(pool=job_entry.pool, job_id=job_entry.id)
        job_entry.status = JobEntry.STATUS_ENQUEUED
        job_entry.save(only=[JobEntry.status])

    def can_enqueue(self, job_entry, current):
        if current['total'] >= self.max_jobs_limit:
            return False
        limit_by_queue = current['by_queue'].get(job_entry.queue, 0)
        if job_entry.queue in self.queue_limits and limit_by_queue >= self.queue_limits[job_entry.queue]:
            return False
        limit_by_pool = current['by_pool'].get(job_entry.pool, 0)
        if job_entry.pool in self.pool_limits and limit_by_pool >= self.pool_limits[job_entry.pool]:
            return False
        return True

    def run(self):
        last_update_limits = 0
        update_limits_time = int(self.section.get(
            self.config.WHIP_LIMITS_UPDATE_TIME, self.config.WHIP_LIMITS_UPDATE_TIME_DEFAULT
        ))
        time_to_sleep = int(self.section.get(self.config.WHIP_CHECK_TIME, self.config.WHIP_CHECK_TIME_DEFAULT))
        loop_sleep_time = 0
        self.whip_log('Whip stared')
        self.whip_log('Jobs check time: {}s'.format(time_to_sleep))
        self.whip_log('Update queue limits time: {}s'.format(update_limits_time))

        while True:
            next_run = time.time() + time_to_sleep
            if loop_sleep_time > 0:
                # Спим только если предыдущее выполнение было короче time_to_sleep
                time.sleep(loop_sleep_time)

            # Обновляем лимиты, если пришо время
            if last_update_limits < (time.time() - update_limits_time):
                self.update_limits()
                last_update_limits = time.time()

            # Получаем информацию о текущих задачах
            current = self.load_current_jobs()

            if current['total'] >= self.max_jobs_limit:
                # Превышено максимальное количество задач
                continue

            # Отправляем на выполнение новые задачи
            for job_entry in JobEntry.select(
                JobEntry.id, JobEntry.pool, JobEntry.queue, JobEntry.status
            ).where(
                (JobEntry.status << [JobEntry.STATUS_NEW, JobEntry.STATUS_RESTART]) 
                & (JobEntry.host == self.host)
                & (JobEntry.execute_at <= datetime.datetime.now())
            ).order_by(JobEntry.execute_at.asc()):
                if self.can_enqueue(job_entry=job_entry, current=current):
                    self.enqueue_job(job_entry=job_entry)
                    current['total'] += 1
                    current['by_queue'][job_entry.queue] += 1
                    current['by_pool'][job_entry.pool] += 1

            # Компенсируем время, затраченное на выполнение
            loop_sleep_time = next_run - time.time()
