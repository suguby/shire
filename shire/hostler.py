# -*- coding: utf-8 -*-
import datetime
import time
import sys

from shire.models import db, JobEntry
from shire.utils import check_pid_is_shire, create_console_handler, create_logger


__all__ = ['Hostler']


class Hostler(object):
    # Специально написан по методологии "let it crash".
    # В случае непредвиденных ситуаций должен быть перезапущен супервайзером, а не пытаться разрешить их самостоятельно,
    # в ущерб стабильности

    CHECK_MINUTES = 5  # Задачи не обновлявшиеся с какого времени проверять

    def __init__(self, config, verbose=False):
        self.config = config
        self.verbose = verbose
        self.hostler_logger = create_logger('shire.hostler')
        if verbose:
            create_console_handler(self.hostler_logger)
        self.section = self.config.section_getter(self.config.HOSTLER_SECTION)
        db.initialize(self.config.get_db())
        self.host = self.config.get(self.config.SHIRE_SECTION, {}).get(
            self.config.SHIRE_HOST, self.config.SHIRE_HOST_DEFAULT
        )
        self.already_checked = {}

    def hostler_log(self, msg, level='info'):
        if not self.verbose:
            return
        getattr(self.hostler_logger, level)(msg)

    def restart_job(self, job_entry):
        self.hostler_log('Task #{} restarted'.format(job_entry.id))
        job_entry.status = JobEntry.STATUS_RESTART
        job_entry.save(only=[JobEntry.status])

    def loop(self):
        last_updated = datetime.datetime.now() - datetime.timedelta(minutes=self.CHECK_MINUTES)
        for job_entry in JobEntry.select(JobEntry.id, JobEntry.status, JobEntry.worker_pid).where(
                                (JobEntry.status == JobEntry.STATUS_IN_PROGRESS) &
                                (JobEntry.updated_at < last_updated) & (JobEntry.host == self.host)
        ):
            if job_entry.id not in self.already_checked or self.already_checked[job_entry.id] < last_updated:
                if not check_pid_is_shire(job_entry.worker_pid):
                    self.restart_job(job_entry)
                self.already_checked[job_entry.id] = datetime.datetime.now()
        time.sleep(float(
            self.section.get(self.config.HOSTLER_CHECK_TIME, self.config.HOSTLER_CHECK_TIME_DEFAULT)
        ))

    def run(self):
        self.hostler_log('Hostler started')
        while True:
            try:
                self.loop()
            except Exception as e:
                self.hostler_log(e, 'exception')
                sys.exit(1)
