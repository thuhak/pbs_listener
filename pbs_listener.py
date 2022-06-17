#!/usr/bin/python
"""
handle pbs job info when job is done
"""
# author: thuhak.zhou@nio.com
import json
import os
import re
import logging
import select
from concurrent.futures import ThreadPoolExecutor
from collections import namedtuple, UserDict
from collections.abc import Iterable
from datetime import datetime
from argparse import ArgumentParser

import psycopg2
import psycopg2.extensions
from dateutil.relativedelta import relativedelta
from myconf import Conf

import extensions


__version__ = '1.1.0'
__author__ = 'thuhak.zhou@nio.com'

logger = logging.getLogger('pbs_listener')
host_pat = re.compile(r'([a-z0-9-]+)(?:\[\d+])?/\d+(?:\*\d+)?')
size_pat = re.compile(r'^(?P<size>\d+)(?P<unit>k|m|g|t)b$', flags=re.IGNORECASE)
arg_pat = re.compile(r'<jsdl-hpcpa:Argument>(.+?)</jsdl-hpcpa:Argument>')
KeyType = namedtuple('KeyType', ['key', 'type'])


# parse PBS attribute
def hours(pat: str) -> float:
    hour, minute, _ = pat.split(':')
    return round(float(hour) + float(minute) / 60, 2)


def seconds(pat: str) -> int:
    hour, minute, second = [int(x) for x in pat.split(':')]
    return hour * 3600 + minute * 60 + second


def timestamp(pat: str) -> datetime:
    return datetime.fromtimestamp(int(pat))


def is_job_success(pat: str) -> bool:
    return not bool(int(pat))


def var_list(pat: str) -> dict:
    l = [i.split('=') for i in pat.strip().split(",")]
    return {i[0]: i[1] for i in l if not i[0].startswith("PBS_")}


def hosts(pat: str) -> list:
    return list(set(host_pat.findall(pat)))


def size(pat: str) -> float:
    """
    :param pat: x(unit)
    :return: y(gb)
    """
    rate_map = {
        'k': -2,
        'm': -1,
        'g': 0,
        't': 1,
    }
    num, unit = size_pat.match(pat).groups()
    n = int(num)
    u = unit.lower()
    rate = rate_map[u]
    return round(n * 1024 ** rate, 3)


def sub_args(pat: str) -> list:
    return arg_pat.findall(pat)


def basename(pat: str) -> str:
    return os.path.basename(pat).rsplit('.', maxsplit=1)[0]


def time_range(start: datetime, finish: datetime) -> float:
    return round((finish - start).total_seconds()/3600, 2)


class JobData(UserDict):
    """
    parse pbs job data
    """
    export_table = {
        'job_name': KeyType('Job_Name', str),
        'project': KeyType('project', str),
        'user': KeyType('euser', str),
        'email': KeyType('Mail_Users', str),
        'queue': KeyType('queue', str),
        'priority': KeyType('Priority', int),
        'run_count': KeyType('run_count', int),
        'cores': KeyType('resources_used.ncpus', int),
        'memory': KeyType('resources_used.mem', size),
        'cpu_hours': KeyType('resources_used.cput', hours),
        'is_job_success': KeyType('Exit_status', is_job_success),
        'hosts': KeyType('exec_host', hosts),
        'create_time': KeyType('ctime', timestamp),
        'finish_time': KeyType('mtime', timestamp),
        'eligible_time': KeyType('etime', timestamp),
        'wait_seconds': KeyType('eligible_time', seconds),
        'run_seconds': KeyType('resources_used.walltime', seconds),
        'args': KeyType('Submit_arguments.', sub_args),
        'variables': KeyType('Variable_List', var_list),
        'placement_set': KeyType('pset', var_list),
        'app': KeyType('executable', basename)
    }

    def __init__(self, job_id, attr, *args, **kwargs):
        super(JobData, self).__init__()
        job_id = int(job_id.split('.', maxsplit=1)[0])
        data = {'job_id': job_id}
        logger.info(f'handle event for job: {job_id}')
        # parse raw_data
        for k, v in self.export_table.items():
            key = v.key if '.' in v.key else f'{v.key}.'
            try:
                raw_data = attr[key]
                if raw_data:
                    d = raw_data.split('.', maxsplit=1)[-1]
                    data[k] = v.type(d)
            except KeyError:
                logger.warning(f"invalid key {key}")
            except Exception as e:
                logger.error(f'error key {key}, {str(e)}')
        # fix data
        data['start_time'] = data['eligible_time'] + relativedelta(seconds=data['wait_seconds'])
        data['run_hours'] = round(data['run_seconds'] / 3600, 2)
        data['wait_hours'] = round(data['wait_seconds'] / 3600, 2)
        data['total_hours'] = time_range(data['create_time'], data['finish_time'])
        data['wait_rate'] = round(data['wait_hours'] / (data['total_hours'] + 0.1), 2)
        if not data.get('app'):
            try:
                data['app'] = basename(data['args'][-1])
            except Exception:
                data['app'] = 'unknown'
        data['job_file'] = data['variables'].get('jobfile', None)
        self.data = data

    def export(self, keys: Iterable) -> dict:
        return {k: v for k, v in self.data.items() if k in keys}


class PBSListener:
    """
    listen to pbs database, handle db events
    """
    channels = ['job']

    def __init__(self, dsn: str, cluster: str, workers=2, **modules):
        """
        :param dsn: pbs postgres dsn
        :param cluster: pbs cluster name
        :param datastore: elasticsearch
        :param user_db: neo4j
        :param mail_sender: smtp
        :param workers: number of threads
        """
        self.dsn = dsn
        self.cluster = cluster
        self.modules = modules
        logger.debug(f'modules loaded: {list(modules.keys())}')
        self.pool = ThreadPoolExecutor(max_workers=workers) if workers else None

    def __enter__(self):
        self.connection = psycopg2.connect(self.dsn)
        logger.info('connecting to pbs database')
        self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            self.pool.shutdown(wait=True)
        self.cursor.close()
        self.connection.close()

    def job_handler(self, payload):
        msg = json.loads(payload)
        jobdata = JobData(*msg)
        jobdata['cluster'] = self.cluster
        logger.debug(f'message received: {jobdata}')
        if user_db := self.modules.get('UserInfo'):
            user = jobdata['user']
            user_info = user_db(user)
            jobdata.update(user_info)
        if mail_sender := self.modules.get('Mail'):
            mail_sender(jobdata)
        if metrics := self.modules.get('Metrics'):
            metric = metrics(jobdata)
            jobdata.update(metric)
        if datastore := self.modules.get('DataStore'):
            datastore(jobdata)

    def run(self):
        for chan in self.channels:
            logger.info(f'listening to channel {chan}')
            self.cursor.execute(f'LISTEN {chan};')
        while True:
            try:
                select.select([self.connection], [], [])
                self.connection.poll()
                while listener := self.connection.notifies:
                    data = listener.pop()
                    channel = data.channel
                    handler = getattr(self, f'{channel}_handler', None)
                    if handler:
                        if self.pool:
                            self.pool.submit(handler, data.payload)
                        else:
                            handler(data.payload)
                    else:
                        logger.error(f'not valid handler for {channel}')
            except KeyboardInterrupt:
                logger.warning('end listening')
                break


if __name__ == '__main__':
    parser = ArgumentParser(description='pbs event handler')
    parser.add_argument('-c', '--config', default='/etc/pbs_listener.yaml', help='config file')
    parser.add_argument('-v', '--verbose', action='store_true', help='debug mode')
    args = parser.parse_args()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level)
    config = Conf(args.config, refresh=False)

    try:
        logger.debug('reading config')
        db_args = ' '.join([f'{x}={y}' for x, y in config['database'].items()])
        logger.debug(f'db args are {db_args}')
        cluster = config['pbs']['cluster']
    except Exception as e:
        logger.error('config error')
        exit(1)
    modules = {}
    for ext in extensions.__all__:
        logger.info(f'loading config for {ext}')
        try:
            if ext_config := config.get(ext):
                modules[ext] = getattr(extensions, ext)(**ext_config)
            else:
                logger.warning(f'skip module {ext}')
        except:
            logger.error(f'can not load {ext}')
    j = PBSListener(db_args, cluster, workers=0, **modules)
    with j:
        j.run()
