# coding: utf-8

'''股票数据采集器
'''

import logging
from datetime import datetime
from distutils.version import LooseVersion

import tushare
from pymongo import MongoClient
from pymongo.errors import WriteError
from copy import deepcopy


MIN_TUSHARE_VERSION = '1.2.10'


class TushareMongodbBaseCollector(object):
    '''从tushare采集并保存到mongodb的基类
    '''

    def __init__(self, token, server_ip, server_port,
                 username, password, database_name, primary_key,
                 insert_update_time=False, log_collection_operation=True):
        self._token = token
        self._server_ip = server_ip
        self._server_port = server_port
        self._username = username
        self._password = password
        self._database_name = database_name
        self._primary_key = primary_key
        self._insert_update_time = insert_update_time
        self._log_collection_operation = log_collection_operation
        self._collection_log = None
        self._collection = None

    def getToken(self):
        return self._token

    def getServerIP(self):
        return self._server_ip

    def getServerPort(self):
        return self._server_port

    def getUsername(self):
        return self._username

    def getPassword(self):
        return self._password

    def getDatabaseName(self):
        return self._database_name

    def getCollection(self):
        return self._collection

    def getTushare(self, token=None):
        # tushare pro api用法查看https://tushare.pro/document/1?doc_id=40
        if not token:
            token = self.getToken()

        assert LooseVersion(tushare.__version__) > \
            LooseVersion(MIN_TUSHARE_VERSION)

        tushare.set_token(token)
        return tushare.pro_api()

    def compareRecord(self, new_record, old_record, with_update_time=False):
        if not old_record:
            return True if new_record else False

        old_record = deepcopy(old_record)
        new_record = deepcopy(new_record)

        old_record.pop('_id')
        if not with_update_time:
            old_record.pop('update_time')
            new_record.pop('update_time')
        return True if set(old_record.items()) ^ set(new_record.items()) else False

    def getRecordId(self, record):
        record_id = deepcopy(self._primary_key)
        for key in record_id:
            record_id['key'] = record[key]
        return record_id

    def updateCollection(self, data):
        '''更新数据到MongoDB数据库集合中
        '''
        if (not data) or (not self.getCollection()):
            return

        now = datetime.now()
        insert_count = replace_count = 0

        for record in data:
            if self._insert_update_time:
                record['update_time'] = now

            record_id = self.getRecordId(record)
            old_record = self.getCollection().find_one(record_id)

            try:
                result = self.getCollection().replace_one(record_id, record, True)
            except WriteError as e:
                logging.error('{}\nrecord_id:{}\nold_record: {}\nreocrd:{}\n'.format(
                    e, record_id, old_record, record))
                raise e

            if (not old_record) or self.compareRecord(record, old_record):
                operation = 'insert' if not old_record else 'replace'
                if operation == 'insert':
                    insert_count += 1
                else:
                    replace_count += 1
                self.logOperation(operation, result.acknowledged, (old_record, record, result))

        return (insert_count, replace_count)

    def openCollection(self, collection_name, validator=None,
                       server_ip=None, server_port=None, username=None,
                       password=None, database_name=None, drop_old=False):
        '''打开数据库集合
        '''
        if not server_ip:
            server_ip = self.getServerIP()
        if not server_port:
            server_port = self.getServerPort()
        if not username:
            username = self.getUsername()
        if not password:
            password = self.getPassword()
        if not database_name:
            database_name = self.getDatabaseName()

        # 连接数据库
        client = MongoClient(server_ip, server_port,
                             username=username,
                             password=password,
                             authSource=database_name)
        db = client[database_name]
        collections = db.list_collection_names()

        if drop_old and (collection_name in collections):
            db[collection_name].drop()
            collections = db.list_collection_names()

        if not (collection_name in collections) and validator:
            collection = db.create_collection(collection_name,
                                              validator=validator)

        collection = db[collection_name]

        if self._primary_key:
            collection.create_index(self._primary_key, unique=True)

        if self._log_collection_operation:
            self._collection_log = db[collection_name + '_log']
            self._collection_log.create_index({'time': 1})

        self._collection = collection

    def logOperation(self, operation, result, detail):
        now = datetime.now()
        if operation == 'insert':
            _, new_record, command_result = detail
            logging.debug('{} | ok: {} | {} | {}'.format(
                operation, result, now, new_record))

        elif operation == 'replace':
            old_record, new_record, command_result = detail
            self._collection_log.insert({
                'time': now, 'operation': operation, 'ok': result,
                'old': old_record})
            logging.debug('{} | ok: {} | {} | old: {} | new: {}'.format(
                operation, result, now, old_record, new_record))

        elif operation == 'delete':
            old_record, _, command_result = detail
            self._collection_log.insert({
                'time': now, 'operation': operation, 'ok': result,
                'old': old_record})
            logging.debug('{} | ok: {} | {} | old: {}'.format(
                operation, result, now, old_record))

    def update(self):
        raise NotImplementedError('update not implemented!')

    def deleteOldRecordsByUpdateTime(self, t):
        if not self._insert_update_time:
            return
        if not self._collection:
            return

        delete_count = 0
        for record in self._collection.find({'update_time': {'$lt': t}}):
            result = self._collection.delete_one(self.getRecordId(record))
            delete_count += 1
            self.logOperation('delete', result.acknowledged, (record, None, result))
