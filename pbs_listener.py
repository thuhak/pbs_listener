#!/usr/bin/python
"""
handle pbs job info when job is done
"""
# author: thuhak.zhou@nio.com
import json
import logging
import select
from concurrent.futures import ThreadPoolExecutor
from argparse import ArgumentParser

import psycopg2
import psycopg2.extensions
from myconf import Conf

from extensions import *

__version__ = '1.0.0'
__author__ = 'thuhak.zhou@nio.com'

logger = logging.getLogger('pbs_listener')


class PBSListener:
    """
    listen to pbs database, handle db events
    """
    channels = ['job']

    def __init__(self, dsn: str,
                 cluster: str,
                 datastore: DataStore = None,
                 user_db: UserInfo = None,
                 mail_sender: Mail = None,
                 metrics: Metrics = None,
                 workers=4):
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
        self.datastore = datastore
        self.user_db = user_db
        self.mail_sender = mail_sender
        self.metrics = metrics
        self.pool = ThreadPoolExecutor(max_workers=workers)

    def __enter__(self):
        self.connection = psycopg2.connect(self.dsn)
        logger.info('connecting to pbs database')
        self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self.connection.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()

    def job_handler(self, payload):
        msg = json.loads(payload)
        logger.debug(f'message received: {msg}')
        jobdata = JobData(*msg)
        jobdata['cluster'] = self.cluster
        user = jobdata['user']
        user_info = self.user_db(user)
        jobdata.update(user_info)
        self.mail_sender(jobdata)
        metrics = self.metrics(jobdata)
        jobdata.update(metrics)
        self.datastore(jobdata)

    def run(self):
        for chan in self.channels:
            logger.info(f'listening to channel {chan}')
            self.cursor.execute(f'LISTEN {chan};')
        with self.pool:
            while True:
                try:
                    select.select([self.connection], [], [])
                    self.connection.poll()
                    while listener := self.connection.notifies:
                        data = listener.pop()
                        channel = data.channel
                        handler = getattr(self, f'{channel}_handler', None)
                        if handler:
                            # handler(data.payload)
                            self.pool.submit(handler, data.payload)
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
        user_db = UserInfo(**config['user_db'])
        mail_sender = Mail(**config['smtp'])
        metrics = Metrics(**config['metrics'])
        datastore = DataStore(**config['datastore'])
    except Exception as e:
        logger.error('config error')
        exit(1)
    j = PBSListener(db_args, cluster, user_db=user_db, mail_sender=mail_sender, datastore=datastore, metrics=metrics)
    with j:
        j.run()
