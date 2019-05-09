# coding: utf-8
'''交易日历
https://tushare.pro/document/2?doc_id=26
'''

import logging
from datetime import datetime, timedelta
from pymongo import DESCENDING, ASCENDING
from .base_collector import TushareMongodbBaseCollector
from ..utils import singleton


@singleton
class TradeCalCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='trade_cal',
            primary_key={'exchange': 1, 'cal_date': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    def getTradeCal(self, exchange='SSE', is_open=None,
                    start_date=None, end_date=None):
        tushare = self.getTushare()
        data = tushare.trade_cal(
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            is_open=is_open,
            fields='exchange,cal_date,is_open,pretrade_date')
        return data.to_dict(orient='records')

    # 不替换旧记录
    def compareRecord(self, new_record, old_record):
        return 0

    def update(self, start_date=None):
        logging.info('update 交易日历(trade_cal) ...')

        self.openCollection()
        checked_total = inserted_total = replaced_total = 0
        for exchange in ['SSE', 'SZSE']:
            start_date = start_date if start_date else '%04d0101' % (datetime.now().year - 20)
            end_date = datetime.now().strftime('%Y%m%d')

            cursor = self.collection.find({'exchange': exchange})

            cursor.sort('cal_date', DESCENDING).limit(1)
            max_date = None
            try:
                max_date = cursor[0]['cal_date']
            except IndexError:
                pass

            cursor.sort('cal_date', ASCENDING).limit(1)
            min_date = None
            try:
                min_date = cursor[0]['cal_date']
            except IndexError:
                pass

            logging.debug('min_date:{}'.format(min_date))
            logging.debug('max_date:{}'.format(max_date))

            if min_date:
                if start_date >= min_date and end_date <= max_date:
                    continue
                elif start_date >= min_date and end_date > max_date:
                    start_date = max_date

            end_date = datetime.now() + timedelta(days=100)
            end_date = end_date.strftime('%Y%m%d')

            logging.info('update {}-{} trade_cal of {} from tushare'.format(
                start_date, end_date, exchange))

            data = self.getTradeCal(exchange=exchange,
                                    start_date=start_date, end_date=end_date)
            data = sorted(data, lambda x: x['cal_date'])

            inserted_count, replaced_count = self.updateCollection(data)

            checked_total += len(data)
            inserted_total += inserted_count
            replaced_total += replaced_count

        logging.info(
            'update trade_cal finished: {} checked, {} inserted, {} replaced'
            .format(checked_total, inserted_total, replaced_total))
