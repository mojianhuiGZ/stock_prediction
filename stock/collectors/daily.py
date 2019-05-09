# coding: utf-8
'''日线行情
https://tushare.pro/document/2?doc_id=27
'''

import time
import logging
from datetime import datetime
from .collectors import TushareMongodbBaseCollector
from .stock_basic import StockBasicCollector
from .trade_cal import TradeCalCollector
from .utils import singleton, getDayRange


@singleton
class DailyCollector(TushareMongodbBaseCollector):

    def __init__(self, token, server_ip, server_port, username, password,
                 database_name):
        super().__init__(
            token,
            server_ip,
            server_port,
            username,
            password,
            database_name,
            primary_key={'ts_code': 1, 'trade_date': 1},
            collection_name='daily',
            get_ts_code_from=StockBasicCollector().getCollectionName(),
            get_open_days_from=TradeCalCollector().getCollectionName(),
        )
        self.validator = None

    def getDaily(self, ts_code=None, trade_date=None, start_date=None, end_date=None):
        tushare = self.getTushare()
        data = tushare.daily(
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=
            'ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
        )
        return data.to_dict(orient='records')

    # 不替换旧记录
    def compareRecord(self, new_record, old_record):
        return 0

    def updateByDate(self):
        ts_code = '601988.SH' # 中国银行

        stock_days = set()
        for doc in self.getCollection().find({'ts_code': ts_code}, {'trade_date': 1}):
            stock_days.add(doc['trade_date'])

        max_day = None
        try:
            max_day = max(stock_days)
        except ValueError:
            pass

        start_date = max_day if max_day else '%04d0101' % (datetime.now().year - 20)

        inserted_total = replaced_total = checked_total = 0

        for day in getDayRange(start_date):
            logging.debug('get {} daily from tushare'.format(day))

            data = None
            delay_time = 61
            while True:
                try:
                    data = self.getDaily(trade_date=day)
                    break
                except Exception as e:
                    logging.debug(e)
                    time.sleep(delay_time)
                    delay_time += 30

            if not data:
                continue

            inserted_count, replaced_count = self.updateCollection(data)
            checked_total += len(data)
            inserted_total += inserted_count
            replaced_total += replaced_count

        return checked_total, inserted_count, replaced_total

    def update(self, mode='trade_date'):
        logging.info('update 日线行情(daily) ...')
        checked_total, inserted_total, replaced_total = self.updateByDate()
        logging.info(
            'update daily finished: {} checked, {} inserted, {} replaced'
            .format(checked_total, inserted_total, replaced_total))
