# -*- coding: utf-8 -*-
import codecs

import click
import os
import sys
import tabulate

from shire.config import Config
from shire.hostler import Hostler
from shire.models import JobEntry, Queue, Limit, Crontab
from shire.manager import ShireManager
from shire.pool import Pool
from shire.pool_starter import PoolStarter
from shire.scribe import Scribe
from shire.whip import Whip
from shire.utils import to_list, is_venv

__all__ = ['cli']

SHIRE_MODELS = (JobEntry, Queue, Limit, Crontab)  # TODO автогенерить?


@click.group()
@click.option('-c', '--config', type=click.Path(), default='shire.cfg')
@click.option('-v', '--verbose', is_flag=True)
@click.pass_context
def cli(ctx, config, verbose):
    config = os.path.realpath(config)
    cfg = Config()
    ctx.obj = ctx.obj or {}
    if os.path.exists(config):
        cfg.load(config)
        ctx.obj['config_path'] = config
    else:
        cfg.make_default()
        ctx.obj['config_path'] = None
        ctx.obj['requested_config_path'] = config
    ctx.obj.update({'cfg': cfg, 'verbose': verbose})


@cli.command()
@click.pass_context
def init_db(ctx):
    with ctx.obj['cfg'].with_db():
        for model in SHIRE_MODELS:
            model.create_table(fail_silently=True)
            print('Table {} created'.format(model.__name__))


@cli.command()
@click.pass_context
def drop_db(ctx):
    with ctx.obj['cfg'].with_db():
        for model in SHIRE_MODELS:
            model.drop_table(fail_silently=True)
            print('Table {} dropped'.format(model.__name__))


@cli.command()
@click.option('--type', 'db_type_arg', default=None)
@click.option('--name', 'db_name_arg', default=None)
@click.option('--user', 'db_user_arg', default=None)
@click.option('--password', 'db_password_arg', default=None)
@click.option('--host', 'db_host_arg', default=None)
@click.option('--port', 'db_port_arg', default=None)
@click.option('--redis-host', 'redis_host_arg', default=None)
@click.option('--redis-port', 'redis_port_arg', default=None)
@click.option('--redis-db', 'redis_db_arg', default=None)
@click.option('--sys_path', 'sys_path_arg', default=None)
@click.option('--venv_path', 'venv_path_arg', default=None)
@click.option('--force', default=False, is_flag=True)
@click.pass_context
def create_config(
        ctx, db_type_arg, db_name_arg, db_user_arg, db_password_arg, db_host_arg, db_port_arg,
        redis_host_arg, redis_port_arg, redis_db_arg, sys_path_arg, venv_path_arg, force
):
    config_path = ctx.obj['config_path'] or ctx.obj['requested_config_path'] or 'shire.cfg'
    if os.path.exists(config_path):
        if not force and not click.confirm('Config already exists. Overwrite?', default=False):
            return
    db_types = {'0': 'postgres', '1': 'sqlite', '2': 'mysql'}

    if db_type_arg is None:
        result = choices_prompt('Database type', db_types)
        db_type = db_types[result]
    else:
        db_type = db_type_arg
    click.echo('{} choosed.'.format(db_type))
    db_name = (
        click.prompt('Database', default='shire.db' if db_type == 'sqlite' else 'shire_db')
        if db_name_arg is None else db_name_arg
    )
    if db_type == 'sqlite':
        db_url = 'sqlite:///{}'.format(db_name)
    else:
        user = click.prompt('User', default='shire_user') if db_user_arg is None else db_user_arg
        password = click.prompt('Password', hide_input=True) if db_password_arg is None else db_password_arg
        host = click.prompt('Host', default='127.0.0.1') if db_host_arg is None else db_host_arg
        port = (
            click.prompt(u'Port', default='5432' if db_type == 'postgres' else '3306')
            if db_port_arg is None else db_port_arg
        )
        db_url = '{}://{}:{}@{}:{}/{}'.format(db_type, user, password, host, port, db_name)

    redis_host = (
        click.prompt('Redis host', default='127.0.0.1') if redis_host_arg is None else redis_host_arg
    )
    redis_port = (
        click.prompt('Redis port', default='6379') if redis_port_arg is None else redis_port_arg
    )
    redis_db = (
        click.prompt('Redis DB', default='0') if redis_db_arg is None else redis_db_arg
    )
    redis_url = 'redis://{}:{}/{}'.format(redis_host, redis_port, redis_db)

    default_path = os.path.abspath('.')
    sys_path = (
        click.prompt(
            u'PYTHON_PATH for project (Use : as delimiter)', default=default_path, type=click.Path(
                exists=True, file_okay=False, writable=False, readable=True
            )
        ) if sys_path_arg is None else sys_path_arg
    )
    default_venv = os.path.dirname(os.path.dirname(sys.executable)) if is_venv() else ''
    venv_path = (
        click.prompt(
            u'Virtualenv path (Leave blank for disable){}'.format(
                u'[Current venv: {}]'.format(default_venv) if default_venv else ''
            ), default='', type=click.Path(
                exists=True, file_okay=False, writable=False, readable=True
            )
        ) if venv_path_arg is None else venv_path_arg
    )

    # Тут записать все в новый конфиг
    cfg = Config()
    cfg.make_default()
    cfg[Config.SHIRE_SECTION][Config.SHIRE_SYS_PATH] = sys_path
    cfg[Config.SHIRE_SECTION][Config.SHIRE_VENV_PATH] = venv_path
    cfg[Config.CONNECTION_SECTION][Config.CONNECTION_DB_URL] = db_url
    cfg[Config.CONNECTION_SECTION][Config.CONNECTION_REDIS_URL] = redis_url

    config = cfg.to_string_io().getvalue()

    if click.confirm(u'Edit config (some parameters nave been set to default)?', default=True):
        edited = click.edit(config)
        if edited is not None:  # None - если редактирование отменено
            config = edited
    with codecs.open(config_path, 'w', 'utf-8') as f:
        f.write(config)
    click.echo(u'Config successfully saved as {}!'.format(config_path))


@cli.command()
@click.option('-n', '--name', type=click.STRING)
@click.pass_context
def run_pool(ctx, name):
    pool = Pool(config=ctx.obj['cfg'], name=name, verbose=ctx.obj['verbose'])
    pool.run()


@cli.command()
@click.option('-n', '--names', type=click.STRING)
@click.pass_context
def start_pools(ctx, names):
    starter = PoolStarter(config=ctx.obj['cfg'], pools=to_list(names), config_path=ctx.obj['config_path'])
    starter.run()


@cli.command()
@click.pass_context
def run_whip(ctx):
    whip = Whip(config=ctx.obj['cfg'], verbose=ctx.obj['verbose'])
    whip.run()


@cli.command()
@click.pass_context
def run_hostler(ctx):
    hostler = Hostler(config=ctx.obj['cfg'], verbose=ctx.obj['verbose'])
    hostler.run()


@cli.command()
@click.pass_context
def run_scribe(ctx):
    scribe = Scribe(config=ctx.obj['cfg'], verbose=ctx.obj['verbose'])
    scribe.run()


@cli.command()
@click.argument('command')
@click.option('-p', '--pools', type=click.STRING, default=None)
@click.option('-s', '--statuses', type=click.STRING, default=None)
@click.pass_context
def manager(ctx, command, pools, statuses):
    shire_manager = ShireManager(ctx.obj['cfg'])
    kwargs = {}
    if pools is not None:
        kwargs['pools'] = to_list(pools)
    if statuses is not None:
        kwargs['from_statuses'] = to_list(statuses.upper())
    if command == 'get_status':
        click.echo(
            tabulate.tabulate(
                [
                    (x['name'], x['uuid'], x['status'])
                    for x in sorted(shire_manager.run_command(command, **kwargs), key=lambda x: x['name'])
                ],
                headers=['Name', 'UUID', 'Status'], tablefmt='psql'
            )
        )
        return
    shire_manager.run_command(command, **kwargs)


@cli.command()
@click.option('-d', '--days_ago', type=click.INT, default=7)
@click.pass_context
def cleanup_old_jobs(ctx, days_ago):
    with ctx.obj['cfg'].with_db():
        from shire.utils import cleanup_old_jobs
        cleanup_old_jobs(days_ago=days_ago)


@cli.command()
@click.argument('job_id', type=click.INT)
@click.pass_context
def execute_job(ctx, job_id):
    with ctx.obj['cfg'].with_db():
        from shire.utils import execute_job
        result = execute_job(job_id, ctx.obj['cfg'])
        # TODO: Убедиться, что это работает
        return 0 if result else 1


@cli.command()
@click.pass_context
@click.argument('action', type=click.STRING)
@click.option('-p', '--pool', type=click.STRING, default='default')
def crontab(ctx, action, pool):
    from shire.scheduler import CrontabJob
    config = ctx.obj['cfg']
    if action == 'start':
        print('Crontab scheduler started!')
    elif action == 'stop':
        CrontabJob.stop_crontab(config=config)
        print('Crontab scheduler stopped!')
    elif action == 'restart':
        CrontabJob.stop_crontab(config=config)
        CrontabJob.start_crontab(config=config, pool=pool)
        print('Crontab scheduler restarted!')
    elif action == 'clear_jobs':
        CrontabJob.stop_crontab(config=config)
        print('Planned crontab jobs cleared')
    elif action == 'list':
        click.echo(
            tabulate.tabulate(
                [
                    (cron.key, cron.cron_string, cron.last_scheduled or '--', cron.description or '')
                    for cron in CrontabJob.list(config=config)
                ],
                headers=['Key', 'Cron String', 'last_scheduled', 'description'], tablefmt='psql'
            )
        )
    else:
        print('Wrong action! May be start/stop/restart/clear_jobs/list')


def choices_prompt(message, choices):
    return click.prompt(
        u'{}\n{}'.format(
            u'\n'.join([u'{} - {}'.format(k, v) for k, v in sorted(choices.items(), key=lambda x: x[0])]),
            message
        ),
        default='0', type=click.Choice(sorted(choices.keys())), show_default=True
    )

if __name__ == '__main__':
    cli(obj={})
