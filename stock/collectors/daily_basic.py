# coding: utf-8
'''每日指标
https://tushare.pro/document/2?doc_id=32
'''

import time
import logging
from datetime import datetime
from .base_collector import TushareMongodbBaseCollector
from .stock_basic import StockBasicCollector
from ..utils import singleton, getDateRange


@singleton
class DailyBasicCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='daily_basic',
            primary_key={'ts_code': 1, 'trade_date': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    def getDailyBasic(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        tushare = self.getTushare()
        data = tushare.daily_basic(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=
            'ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,total_share,float_share,free_share,total_mv,circ_mv'
        )
        return data.to_dict(orient='records')

    # 不替换旧记录
    def compareRecord(self, new_record, old_record):
        return 0

    def update(self):
        logging.info('update 每日指标(daily_basic) ...')

        self.openCollection()
        ts_codes = StockBasicCollector().getStockCodes()

        result = self.collection.aggregate([
            {'$group': {'_id': '$ts_code', 'max_date': {'$max': '$trade_date'}}},
        ])
        start_date = None
        for it in result:
            if not start_date or (it['max_date'] and it['max_date'] < start_date):
                start_date = it['max_date']

        inserted_total = replaced_total = checked_total = 0
        dates = getDateRange(start_date, datetime.now())
        logging.debug('start_date: {}'.format(start_date))

        for date in dates:
            logging.debug('get {} daily_basic from tushare'.format(date))

            data = None
            delay_time = 61
            while True:
                try:
                    data = self.getDailyBasic(trade_date=date)
                    break
                except Exception as e:
                    logging.debug(e)
                    time.sleep(delay_time)
                    delay_time += 30

            if not data:
                continue

            data = sorted(data, key=lambda x: x['trade_date'])
            inserted_count, replaced_count = self.updateCollection(data)

            checked_total += len(data)
            inserted_total += inserted_count
            replaced_total += replaced_count

        logging.info(
            'update daily_basic finished: {} checked, {} inserted, {} replaced'
            .format(checked_total, inserted_total, replaced_total))
