# coding: utf-8
'''股票列表
https://tushare.pro/document/2?doc_id=25
'''

import logging
from .collectors import TushareMongodbBaseCollector


class StockBasicCollector(TushareMongodbBaseCollector):
    collection_name = 'stock_basic'

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
                    'ts_code', 'symbol', 'name', 'area', 'industry',
                    'fullname', 'enname', 'market', 'exchange', 'curr_type',
                    'list_status', 'list_date', 'delist_date', 'is_hs',
                    'update_time'
                ],
                'properties': {
                    'ts_code': {
                        'bsonType': 'string',
                        'title': 'TS代码'
                    },
                    'symbol': {
                        'bsonType': 'string',
                        'title': '股票代码'
                    },
                    'name': {
                        'bsonType': 'string',
                        'title': '股票名称'
                    },
                    'area': {
                        'bsonType': ['string', 'null'],
                        'title': '所在地域'
                    },
                    'industry': {
                        'bsonType': ['string', 'null'],
                        'title': '所属行业'
                    },
                    'fullname': {
                        'bsonType': ['string', 'null'],
                        'title': '股票全称'
                    },
                    'enname': {
                        'bsonType': ['string', 'null'],
                        'title': '英文全称'
                    },
                    'market': {
                        'bsonType': ['string', 'null'],
                        'title': '市场类型'
                    },
                    'exchange': {
                        'bsonType': 'string',
                        'title': '交易所代码'
                    },
                    'curr_type': {
                        'bsonType': 'string',
                        'title': '交易货币'
                    },
                    'list_status': {
                        'bsonType': 'string',
                        'title': '上市状态'
                    },
                    'list_date': {
                        'bsonType': 'string',
                        'title': '上市日期'
                    },
                    'delist_date': {
                        'bsonType': ['string', 'null'],
                        'title': '退市日期'
                    },
                    'is_hs': {
                        'bsonType': 'string',
                        'title': '是否沪深港通标的'
                    },
                    'update_time': {
                        'bsonType': 'date',
                        'title': '更新日期'
                    }
                }
            }
        }

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

        logging.info('get stock_basic from tushare')
        data = self.getStockBasic()

        logging.info('open mongodb database')
        logging.info('mongodb server ip and port: {}:{}'.format(
            self.getServerIP(), self.getServerPort()))
        logging.info('mongodb database and collection: {}.{}'.format(
            self.getDatabaseName(), StockBasicCollector.collection_name))

        collection = self.openDatabase(StockBasicCollector.collection_name, self.validator)

        logging.info('update to mongodb database')

        insert_count, replace_count, delete_count = self.updateDatabase(
            data, collection)

        logging.info(
            'update stock_basic finished: {} checked {} inserted {} replaced {} deleted!'
            .format(len(data), insert_count, replace_count, delete_count))
