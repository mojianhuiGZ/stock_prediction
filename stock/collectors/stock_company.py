# coding: utf-8
'''上市公司基本信息
https://tushare.pro/document/2?doc_id=112
'''

import logging
from .base_collector import TushareMongodbBaseCollector
from ..utils import singleton


@singleton
class StockCompanyCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='stock_company',
            primary_key={'ts_code': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    def getStockCompany(self):
        tushare = self.getTushare()

        data_sse = tushare.stock_company(
            exchange='SSE',
            fields=
            'ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope'
        )
        data_sse = data_sse.to_dict(orient='records')

        data_szse = tushare.stock_company(
            exchange='SZSE',
            fields=
            'ts_code,exchange,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope'
        )
        data_szse = data_szse.to_dict(orient='records')

        return data_sse + data_szse

    def update(self):
        logging.info('update 上市公司基本信息(stock_company) ...')

        data = self.getStockCompany()
        self.openCollection()
        inserted_count, replaced_count = self.updateCollection(data)

        logging.info(
            'update stock_company finished: {} checked, {} inserted, {} replaced'
            .format(len(data), inserted_count, replaced_count))
