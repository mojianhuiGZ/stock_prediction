# coding: utf-8
'''指数日线行情
https://tushare.pro/document/2?doc_id=95
'''

import time
import logging
from datetime import datetime

from pymongo import DESCENDING, ASCENDING

from ..utils import singleton
from .base_collector import TushareMongodbBaseCollector
from .index_basic import IndexBasicCollector


@singleton
class IndexDailyCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='index_daily',
            primary_key={'ts_code': 1, 'trade_date': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    # 需要指定ts_code
    def getIndexDaily(self, ts_code, start_date=None, end_date=None):
        tushare = self.getTushare()
        data = tushare.index_daily(
            ts_code=ts_code,
            trade_date='',
            start_date=start_date,
            end_date=end_date,
            fields=
            'ts_code,trade_date,close,open,high,low,pre_close,change,pct_chg,vol,amount'
        )
        return data.to_dict(orient='records')

    # 不替换旧记录
    def compareRecord(self, new_record, old_record):
        return 0

    def updateByTsCode(self, ts_code, start_date=None):
        if not self.collection:
            raise RuntimeError('collection {} is not open!'.format(self.collection_name))

        end_date = datetime.now().strftime('%Y%m%d')

        cursor = self.collection.find({'ts_code': ts_code})

        cursor.sort('trade_date', DESCENDING).limit(1)
        max_date = None
        try:
            max_date = cursor[0]['trade_date']
        except IndexError:
            pass

        cursor.sort('trade_date', ASCENDING).limit(1)
        min_date = None
        try:
            min_date = cursor[0]['trade_date']
        except IndexError:
            pass

        logging.debug('min_date:{}'.format(min_date))
        logging.debug('max_date:{}'.format(max_date))

        start_date = start_date if start_date else min_date

        if min_date:
            if start_date >= min_date and end_date <= max_date:
                return
            elif start_date >= min_date and end_date > max_date:
                start_date = max_date

        logging.info('update {}-{} index_daily of {} from tushare'.format(
            start_date, end_date, ts_code))

        data = None
        delay_time = 61
        while True:
            try:
                data = self.getIndexDaily(ts_code, start_date, end_date)
                break
            except Exception as e:
                logging.debug(e)
                time.sleep(delay_time)
                delay_time += 30

        if not data:
            return (0, 0, 0)

        data = sorted(data, key=lambda x: x['trade_date'])
        inserted_count, replaced_count = self.updateCollection(data)
        return (len(data), inserted_count, replaced_count)

    def update(self):
        logging.info('update 指数日线行情(index_daily) ...')

        self.openCollection()

        codes = IndexBasicCollector().getIndexCodes()
        counts = {c: 0 for c in codes}
        result = self.collection.aggregate([
            {'$group': {'_id': '$ts_code', 'count': {'$sum': 1}}},
        ])
        for it in result:
            counts[it['_id']] += it['count']
        counts = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        inserted_total = replaced_total = checked_total = 0
        for it in counts:
            ts_code = it[0]
            checked_count, inserted_count, replaced_count = self.updateByTsCode(ts_code)
            checked_total += checked_count
            inserted_total += inserted_count
            replaced_total += replaced_count

        logging.info(
            'update index_daily finished: {} checked, {} inserted, {} replaced'
            .format(checked_total, inserted_total, replaced_total))
