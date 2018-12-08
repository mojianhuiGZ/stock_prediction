# coding: utf-8
'''上市公司基本信息
https://tushare.pro/document/2?doc_id=112
'''

import logging
from math import isnan
from .collectors import TushareMongodbBaseCollector


class StockCompanyCollector(TushareMongodbBaseCollector):
    collection_name = 'stock_company'

    def __init__(self,
                 token,
                 server_ip,
                 server_port,
                 username,
                 password,
                 database_name):
        super().__init__(token, server_ip, server_port, username, password,
                         database_name)

        self.validator = {
            '$jsonSchema': {
                'bsonType':
                'object',
                'required': [
                    'ts_code', 'exchange', 'chairman', 'manager', 'secretary',
                    'reg_capital', 'setup_date', 'province', 'city',
                    'introduction', 'website', 'email', 'office', 'employees',
                    'main_business', 'business_scope', 'update_time'
                ],
                'properties': {
                    'ts_code': {
                        'bsonType': 'string',
                        'title': '股票代码'
                    },
                    'exchange': {
                        'bsonType': 'string',
                        'title': '交易所代码'
                    },
                    'chairman': {
                        'bsonType': ['string', 'null'],
                        'title': '法人代表'
                    },
                    'manager': {
                        'bsonType': ['string', 'null'],
                        'title': '总经理'
                    },
                    'secretary': {
                        'bsonType': ['string', 'null'],
                        'title': '董秘'
                    },
                    'reg_capital': {
                        'bsonType': 'double',
                        'title': '注册资本'
                    },
                    'setup_date': {
                        'bsonType': ['string', 'null'],
                        'title': '注册日期'
                    },
                    'province': {
                        'bsonType': ['string', 'null'],
                        'title': '所在省份'
                    },
                    'city': {
                        'bsonType': ['string', 'null'],
                        'title': '所在城市'
                    },
                    'introduction': {
                        'bsonType': ['string', 'null'],
                        'title': '公司介绍'
                    },
                    'website': {
                        'bsonType': ['string', 'null'],
                        'title': '公司主页'
                    },
                    'email': {
                        'bsonType': ['string', 'null'],
                        'title': '电子邮件'
                    },
                    'office': {
                        'bsonType': ['string', 'null'],
                        'title': '办公室'
                    },
                    'employees': {
                        'bsonType': 'int',
                        'title': '员工人数'
                    },
                    'main_business': {
                        'bsonType': ['string', 'null'],
                        'title': '主要业务及产品'
                    },
                    'business_scope': {
                        'bsonType': ['string', 'null'],
                        'title': '经营范围'
                    },
                    'update_time': {
                        'bsonType': 'date',
                        'title': '更新日期'
                    }
                }
            }
        }

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

        logging.info('get stock_company from tushare')
        data = self.getStockCompany()

        logging.info('open mongodb database')
        logging.info('mongodb server ip and port: {}:{}'.format(
            self.getServerIP(), self.getServerPort()))
        logging.info('mongodb database and collection: {}.{}'.format(
            self.getDatabaseName(), StockCompanyCollector.collection_name))

        collection = self.openDatabase(StockCompanyCollector.collection_name, self.validator)

        logging.info('update to mongodb database')

        insert_count, replace_count, delete_count = self.updateDatabase(
            data, collection)

        logging.info(
            'update stock_company finished: {} checked {} inserted {} replaced {} deleted!'
            .format(len(data), insert_count, replace_count, delete_count))
