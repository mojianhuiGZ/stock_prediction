# coding: utf-8
'''上市公司基本信息
https://tushare.pro/document/2?doc_id=112
'''

import logging
from math import isnan
from .collectors import TushareMongodbBaseCollector


class StockCompanyCollector(TushareMongodbBaseCollector):
    collection_name = 'stock_company'

    def __init__(self, token, server_ip, server_port, username, password,
                 database_name):
        super().__init__(token, server_ip, server_port, username, password,
                         database_name,
                         primary_key={'ts_code': 1},
                         insert_update_time=True,
                         log_collection_operation=True)
        self.validator = None

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

        def fixup(item):
            try:
                if not isinstance(item['employees'], int):
                    item['employees'] = int(item['employees'])
            except ValueError:
                item['employees'] = 0

            # if isnan(item['reg_capital']):
            #     item['reg_capital'] = 0.0

        for item in data_szse:
            fixup(item)

        return data_sse + data_szse

    def update(self):
        logging.info('update 上市公司基本信息(stock_company) ...')

        logging.info('get stock_company from tushare ...')
        data = self.getStockCompany()

        logging.info('open mongodb database ...')
        logging.info('mongodb server ip and port: {}:{}'.format(
            self.getServerIP(), self.getServerPort()))
        logging.info('mongodb database and collection: {}.{}'.format(
            self.getDatabaseName(), StockCompanyCollector.collection_name))

        self.openCollection(StockCompanyCollector.collection_name,
                            self.validator)

        logging.info('update to mongodb database ...')

        insert_count, replace_count = self.updateCollection(data)

        logging.info(
            'update stock_company finished: {} checked, {} inserted, {} replaced'
            .format(len(data), insert_count, replace_count))
