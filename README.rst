Shire
========

Heavy-job queue processor


Installation
--------------------

pip install shire


Create config file
--------------------

Run shire-cli create_config, follow instructions.

An example of the shire.cfg::

    [connection]
    redis_url = redis://127.0.0.1:6379/0
    db_url = postgres://peewee:123@127.0.0.1:5432/peewee
    
    [hostler]
    check_time = 5
    
    [shire]
    venv_path = /path/to/your/virtualenv
    sys_path = /path/to/your/project:/path/to/external/library
    host = default

    [scribe]
    per_pool = 1
    log_max_size = 104857600
    log_directory = logs
    max_logs = 5
    default_log = default.log
    
    [pool]
    check_time = 30
    
    [whip]
    limits_update_time = 60
    check_time = 1


Queue management
--------------------

Interactive shire config creation
  shire-cli -c /path/to/your/shire.cfg create_config

Run whip (Enqueue jobs to pools)
  shire-cli -c /path/to/your/shire.cfg run_whip

Run scribe (Shire job logs writer)
  shire-cli -c /path/to/your/shire.cfg run_scribe

Run hostler (Shire failed jobs restarter)
  shire-cli -c /path/to/your/shire.cfg run_hostler

Run pool (Shire job executor)
  shire-cli -c /path/to/your/shire.cfg run_pool --name=pool_name

Run multiple pools with one master-process
  shire-cli -c /path/to/your/shire.cfg start_pools --names=pool_name,another_pool


Example
-------------------
Run shire
  shire-cli -c /path/to/your/shire.cfg run_whip
  shire-cli -c /path/to/your/shire.cfg run_pool --name=default

Create example jobs module::

    # my_jobs.py
    from shire.job import Job
    import requests

    def save(text_len):
        # save - your external function to save result data. Shire not save any results itself.
        pass

    class CountWordsAtUrl(Job):
        def run(url=None):
            resp = requests.get(url)		
            save(len(resp.text.split()))
            
Create example configuration module::

     # shire_conf.py
     from shire.config import Config as ShireConfig
     conf = ShireConfig()
     conf.load('/path/to/your/shire.cfg')

Enqueue job::
	
    from my_jobs import CountWordsAtUrl
    from shire_conf import conf
    CountWordsAtUrl.delay(conf, pool='default', kwargs={'url': 'https://github.com/suguby/shire'})