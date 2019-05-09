# coding: utf-8
'''分红送股
https://tushare.pro/document/2?doc_id=103
'''

import time
import logging
from .base_collector import TushareMongodbBaseCollector
from .stock_basic import StockBasicCollector
from ..utils import singleton


@singleton
class DividendCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='dividend',
            primary_key={'ts_code': 1, 'end_date': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    def getDividend(self, ts_code):
        tushare = self.getTushare()
        data = tushare.dividend(
            ts_code=ts_code,
            ann_date='',
            record_date='',
            ex_date='',
            fields=
            'ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share'
        )
        return data.to_dict(orient='records')

    # 不替换旧记录
    def compareRecord(self, new_record, old_record):
        return 0

    def update(self):
        logging.info('update 分红送股(dividend) ...')

        ts_codes = StockBasicCollector().getStockCodes()
        self.openCollection()
        inserted_total = replaced_total = checked_total = 0
        for ts_code in ts_codes:
            logging.debug('get dividend of {} from tushare'.format(ts_code))

            data = None
            delay_time = 61
            while True:
                try:
                    data = self.getDividend(ts_code)
                    break
                except Exception as e:
                    logging.debug(e)
                    time.sleep(delay_time)
                    delay_time += 30

            if not data:
                continue

            data = sorted(data, key=lambda x: x['end_date'])
            inserted_count, replaced_count = self.updateCollection(data)

            checked_total += len(data)
            inserted_total += inserted_count
            replaced_total += replaced_count

        logging.info(
            'update dividend finished: {} checked, {} inserted, {} replaced'
            .format(checked_total, inserted_total, replaced_total))
