"""
get metrics from monitoring system
"""
import logging
from collections import namedtuple
from enum import Enum
import statistics

import pyzabbix
import urllib3

from cachetools import TTLCache, cached
from pyzabbix import ZabbixAPI
import requests


logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
session = requests.Session()
session.verify = False
cache = TTLCache(maxsize=100000, ttl=7200)
Item = namedtuple('Item', ['key', 'type'])


class ItemType(Enum):
    Float = 0
    String = 1
    Log = 2
    Uint = 3
    Text = 4


class Metrics:
    _supported_keys = {
        'flops': Item('cpu.flops', ItemType.Float),
        'memory_bandwidth': Item('mem.band', ItemType.Float),
        'ib_send': Item('PortXmitDataps', ItemType.Float),
        'ib_receive': Item('PortRcvDataps', ItemType.Float)
    }

    def __init__(self, url, user, password):
        self.zapi = ZabbixAPI(url, session=session)
        self.auth = (user, password)

    @cached(cache=cache)
    def _host_id(self, host) -> str:
        result = self.zapi.host.get(filter={'name': host}, output=['hostid'])
        assert len(result) == 1, "not valid host"
        host_id = result[0]['hostid']
        return host_id

    @cached(cache=cache)
    def _item_id(self, key: str, host: str) -> str:
        host_id = self._host_id(host)
        result = self.zapi.item.get(hostids=host_id, search={'key_': key}, output=['itemid'])
        assert len(result) == 1, 'not valid item'
        item_id = result[0]['itemid']
        return item_id

    def history(self, key: str, hosts: list, start: int, end: int) -> list:
        item = self._supported_keys[key]
        keyname = item.key
        if item.type is ItemType.Float:
            method = float
        elif item.type is ItemType.Uint:
            method = int
        else:
            method = str
        history_type = item.type.value
        items = [self._item_id(keyname, h) for h in hosts]
        data = self.zapi.history.get(itemids=items,
                                     time_from=start,
                                     time_till=end,
                                     history=history_type,
                                     output=['value'])
        return [method(i['value']) for i in data]

    def __call__(self, jobdata) -> dict:
        result = {}
        time_from = int(jobdata['start_time'].timestamp())
        time_till = int(jobdata['finish_time'].timestamp())
        if time_till - time_from < 500:
            logger.warning('job ended to soon')
            return result
        hosts = jobdata['hosts']
        nodes = len(hosts)
        cores = jobdata['cores']
        try:
            self.zapi.login(*self.auth)
        except pyzabbix.ZabbixAPIException as e:
            logger.error(f'zabbix error: {e}')

        logger.debug('getting flops')
        try:
            flops = self.history('flops', hosts, time_from, time_till)
            flops_per_core = statistics.fmean(flops) * nodes / cores
            result['flops_per_core'] = flops_per_core
        except Exception as e:
            logger.error(f'fail to get flops, {e}')
        return result
