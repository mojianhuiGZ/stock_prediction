# coding: utf-8
'''股票列表
https://tushare.pro/document/2?doc_id=25
'''

import logging
from .collectors import TushareMongodbBaseCollector


class StockBasicCollector(TushareMongodbBaseCollector):
    collection_name = 'stock_basic'

    def __init__(self, token, server_ip, server_port, username, password,
                 database_name):
        super().__init__(token, server_ip, server_port, username, password,
                         database_name,
                         primary_key={'ts_code': 1},
                         insert_update_time=True,
                         log_collection_operation=True)
        self.validator = None

    def getStockBasic(self):
        tushare = self.getTushare()
        data = tushare.stock_basic(
            is_hs='',
            list_status='',
            exchange='',
            fields=
            'ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
        )
        return data.to_dict(orient='records')

    def update(self):
        logging.info('update 股票列表(stock_basic) ...')

        logging.info('get stock_basic from tushare ...')
        data = self.getStockBasic()

        logging.info('open mongodb database ...')
        logging.info('mongodb server ip and port: {}:{}'.format(
            self.getServerIP(), self.getServerPort()))
        logging.info('mongodb database and collection: {}.{}'.format(
            self.getDatabaseName(), StockBasicCollector.collection_name))

        self.openCollection(StockBasicCollector.collection_name, self.validator)

        logging.info('update to mongodb database ...')

        insert_count, replace_count = self.updateCollection(data)

        logging.info(
            'update stock_basic finished: {} checked, {} inserted, {} replaced'
            .format(len(data), insert_count, replace_count))
