from datetime import datetime
import logging

from elasticsearch import Elasticsearch
from dateutil.relativedelta import relativedelta

from . import JobData


logger = logging.getLogger(__name__)


class DataStore:
    """
    target datastore
    """

    def __init__(self, index='hpc_jobs', **kwargs):
        self.es = Elasticsearch(**kwargs)
        self.index = index

    def __call__(self, data: JobData):
        t = datetime.utcnow()
        index = f'{self.index}-{t.year}'
        for k, v in data.items():
            if isinstance(v, datetime):
                data[k] = v - relativedelta(hours=8)
        logger.info(f'save data to elasticsearch')
        try:
            self.es.index(index=index, document=dict(data))
        except Exception as e:
            logger.error(f'elasticsearch error, {e}')