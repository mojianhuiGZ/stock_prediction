# coding: utf-8
'''指数基本信息
https://tushare.pro/document/2?doc_id=94
'''

import logging
from .base_collector import TushareMongodbBaseCollector
from ..utils import singleton


@singleton
class IndexBasicCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='index_basic',
            primary_key={'ts_code': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    def getIndexBasic(self):
        tushare = self.getTushare()
        data = []
        for market in ['MSCI', # MSCI指数
                       'CSI',  # 中证指数
                       'SSE',  # 上交所指数
                       'SZSE', # 深交所指数
                       'CICC', # 中金所指数
                       'SW',   # 申万指数
                       'CNI',  # 国证指数
                       'OTH',  # 其他指数
                       ]:
            data += tushare.index_basic(
                market=market,
                publisher='',
                category='',
                fields=
                'ts_code,name,fullname,market,publisher,index_type,category,base_date,base_point,list_date,weight_rule,desc,exp_date'
            ).to_dict(orient='records')
        return data

    def getIndexCodes(self):
        if not self.collection:
            self.openCollection()

        ts_codes = []
        for doc in self.collection.find({}, {'ts_code': True, '_id': False}):
            ts_codes.append(doc['ts_code'])
        return ts_codes

    def update(self):
        logging.info('update 指数基本信息(index_basic) ...')

        data = self.getIndexBasic()
        self.openCollection()
        inserted_count, replaced_count = self.updateCollection(data)

        logging.info(
            'update index_basic finished: {} checked, {} inserted, {} replaced'
            .format(len(data), inserted_count, replaced_count))
