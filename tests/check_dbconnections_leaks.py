# -*- coding: utf-8 -*-
from __future__ import print_function

import time

from shire.config import Config
from shire.models import db, JobEntry, Limit
from shire.job import Job


class TestDbJob(Job):

    def run(self, sleep=None):
        for row in Limit.select(Limit.entity, Limit.limit,).where(Limit.entity_type == Limit.ENTITY_POOL):
            print(row)
        if sleep:
            time.sleep(sleep)


def main():
    res = ''
    while res not in ('Y', 'y', u'д', u'Д'):
        res = raw_input('U must run whip and pool "test" with config shire.cfg... Done? (y/n)')
    cfg = Config()
    cfg.load('shire.cfg')
    db.initialize(cfg.get_db())

    jobs = []
    print('Run 10 jobs')
    for i in range(10):
        job_descr = TestDbJob.delay(config=cfg, pool='test', kwargs=dict(sleep=1))
        jobs.append(job_descr)
    print('Wait completion')
    while len(jobs):
        for job_descr in jobs[:]:
            if JobEntry.get(JobEntry.id == job_descr.id).status == JobEntry.STATUS_ENDED:
                jobs.remove(job_descr)
        time.sleep(1)
    print('Check DB connects')
    for row in db.execute_sql('SELECT datname, numbackends FROM pg_stat_database'):
        print (row)

main()
