# coding: utf-8
'''股票列表
https://tushare.pro/document/2?doc_id=25
'''

import logging
from .base_collector import TushareMongodbBaseCollector
from ..utils import singleton


@singleton
class StockBasicCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='stock_basic',
            primary_key={'ts_code': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

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

    def getStockCodes(self):
        if not self.collection:
            self.openCollection()

        ts_codes = []
        for doc in self.collection.find({}, {'ts_code': True, '_id': False}):
            ts_codes.append(doc['ts_code'])
        return ts_codes

    def update(self):
        logging.info('update 股票列表(stock_basic) ...')

        data = self.getStockBasic()
        self.openCollection()
        inserted_count, replaced_count = self.updateCollection(data)

        logging.info(
            'update stock_basic finished: {} checked, {} inserted, {} replaced'
            .format(len(data), inserted_count, replaced_count))
