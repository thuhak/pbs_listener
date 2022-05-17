import os
import re
import logging
from datetime import datetime
from collections import namedtuple, UserDict, Iterable


logger = logging.getLogger(__name__)
host_pat = re.compile(r'([a-z0-9-]+)(?:\[\d+])?/\d+(?:\*\d+)?')
size_pat = re.compile(r'^(?P<size>\d+)(?P<unit>k|m|g|t)b$', flags=re.IGNORECASE)
arg_pat = re.compile(r'<jsdl-hpcpa:Argument>(.+?)</jsdl-hpcpa:Argument>')
KeyType = namedtuple('KeyType', ['key', 'type'])


# parse PBS attribute
def hours(pat: str) -> float:
    hour, minute, _ = pat.split(':')
    return round(float(hour) + float(minute) / 60, 2)


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
        'start_time': KeyType('stime', timestamp),
        'eligible_time': KeyType('etime', timestamp),
        # 'wait_hours': KeyType('eligible_time', hours),
        # 'run_hours': KeyType('resources_used.walltime', hours),
        'args': KeyType('Submit_arguments.', sub_args),
        'variables': KeyType('Variable_List', var_list),
        'placement_set': KeyType('pset', var_list),
        'app': KeyType('executable', basename)
    }

    def __init__(self, job_id, attr):
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
        if not data.get('start_time'):
            logger.warning('not start_time in pbs, use eligible_time instead')
            data['start_time'] = data['eligible_time']
        data['run_hours'] = time_range(data['start_time'], data['finish_time'])
        data['wait_hours'] = time_range(data['eligible_time'], data['start_time'])
        data['total_hours'] = time_range(data['create_time'], data['finish_time'])
        data['wait_rate'] = round(data['wait_hours'] / data['total_hours'], 2)
        if not data.get('app'):
            try:
                data['app'] = basename(data['args'][-1])
            except Exception:
                data['app'] = 'unknown'
        data['job_file'] = data['variables'].get('jobfile', None)
        self.data = data

    def export(self, keys: Iterable) -> dict:
        return {k: v for k, v in self.data.items() if k in keys}