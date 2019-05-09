# coding: utf-8

'''股票数据采集器
'''

import logging
from copy import deepcopy
from datetime import datetime
from distutils.version import LooseVersion

import tushare
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure, WriteError

from ..settings import TUSHARE_TOKEN, MONGODB_SERVER_IP, MONGODB_SERVER_PORT, \
    STOCK_DATABASE_USERNAME, STOCK_DATABASE_PASSWORD, STOCK_DATABASE
from ..utils import initLogger


MIN_TUSHARE_VERSION = '1.2.10'


class TushareMongodbBaseCollector():
    '''从tushare采集并保存到mongodb的基类
    '''
    def __init__(self,
                 collection_name,
                 primary_key,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        self.collection_name = collection_name
        self.primary_key = primary_key
        self.token = token if token else TUSHARE_TOKEN
        self.server_ip = server_ip if server_ip else MONGODB_SERVER_IP
        self.server_port = server_port if server_port else MONGODB_SERVER_PORT
        self.username = username if username else STOCK_DATABASE_USERNAME
        self.password = password if password else STOCK_DATABASE_PASSWORD
        self.database_name = database_name if database_name else STOCK_DATABASE
        self.db = None
        self.collection = None
        initLogger()

    def getTushare(self):
        # tushare pro api用法查看https://tushare.pro/document/1?doc_id=40
        assert LooseVersion(tushare.__version__) > \
            LooseVersion(MIN_TUSHARE_VERSION)

        tushare.set_token(self.token)
        return tushare.pro_api()

    def database_info(self):
        result = {}
        result['tushare token'] = self.token
        result['mongodb server ip'] = self.server_ip
        result['mongodb server port'] = self.server_port
        result['mongodb server username'] = self.username
        result['mongodb server password'] = self.password
        result['database'] = self.database_name
        result['collection name'] = self.collection_name
        result['primary key'] = self.primary_key
        return result

    def getExchangeOpenDays(self, exchange, start_date, end_date=None):
        if self._get_open_days_from:
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')

            key = '%s:%s' % (start_date, end_date)
            if key in self._exchange_open_days_cache:
                return self._exchange_open_days_cache[key]

            collection = self.openCollection(self._get_open_days_from)
            result = set()
            for day in collection.find({'exchange': exchange,
                                        'is_open': 1,
                                        'cal_date': {'$lte': end_date, '$gte': start_date},
                                        }):
                result.add(day['cal_date'])
            self._exchange_open_days_cache[key] = result
            return result

    # 不同就替换
    def compareRecord(self, new_record, old_record):
        if not old_record:
            return 1 if new_record else 0

        old_record = deepcopy(old_record)
        new_record = deepcopy(new_record)

        old_record.pop('_id')
        return 1 if set(old_record.items()) ^ set(new_record.items()) else 0

    def getRecordId(self, record):
        return {i[0]: record[i[0]] for i in self.primary_key.items()}

    def updateCollection(self, data):
        '''更新数据到MongoDB数据库集合中
        '''
        if (not data) or (not self.collection):
            return

        inserted_count = replaced_count = 0

        for record in data:
            record_id = self.getRecordId(record)
            old_record = self.collection.find_one(record_id)

            if (not old_record) or self.compareRecord(record, old_record) > 0:
                try:
                    result = self.collection.replace_one(record_id, record, True)
                except WriteError as e:
                    logging.error('{}\nrecord_id:{}\nold_record:{}\nreocrd:{}\n'.format(
                        e, record_id, old_record, record))
                    raise e

                operation = 'insert' if not old_record else 'replace'
                if operation == 'insert':
                    inserted_count += 1
                else:
                    replaced_count += 1

                #self.logOperation(operation, result.acknowledged, (old_record, record, result))

        return (inserted_count, replaced_count)

    def openCollection(self, collection_name=None):
        '''打开数据库集合
        '''
        if not self.db:
            # 连接数据库
            client = MongoClient(self.server_ip, self.server_port,
                                username=self.username,
                                password=self.password,
                                authSource=self.database_name)
            db = client[self.database_name]
            self.db = db
        else:
            db = self.db

        collection_name = collection_name if collection_name else self.collection_name

        if collection_name != self.collection_name:
            return db[collection_name]

        collections = db.list_collection_names()

        if not (collection_name in collections):
            collection = db.create_collection(collection_name)
            logging.debug('create collection {}'.format(collection_name))

        collection = db[collection_name]

        if self.primary_key:
            primary_key = []
            for i in self.primary_key.items():
                primary_key.append((i[0],
                                    DESCENDING if i[1] < 0 else ASCENDING))
            try:
                collection.create_index(primary_key, unique=True)
                logging.debug('{} create_index({}, unique=True)'.format(
                    collection.name, primary_key))
            except OperationFailure as e:
                logging.debug(e)

        self.collection = collection
        return collection

    def logOperation(self, operation, result, detail):
        now = datetime.now()
        if operation == 'insert':
            _, new_record, command_result = detail
            logging.debug('{} | {} | ok: {} | {} | {}'.format(
                self.collection_name, operation, result, now, new_record))

        elif operation == 'replace':
            old_record, new_record, command_result = detail
            logging.debug('{} | {} | ok: {} | {} | old: {} | new: {}'.format(
                self.collection_name, operation, result, now, old_record, new_record))

        elif operation == 'delete':
            old_record, _, command_result = detail
            logging.debug('{} | {} | ok: {} | {} | old: {}'.format(
                self.collection_name, operation, result, now, old_record))

    def update(self):
        raise NotImplementedError('not implemented!')
