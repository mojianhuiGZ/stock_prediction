# coding: utf-8

import logging
import argparse
from pymongo import MongoClient
from stock.utils import initLogger, initDefaultExceptionHandler
from stock.settings import MONGODB_SERVER_IP, MONGODB_SERVER_PORT, \
    STOCK_DATABASE_USERNAME, STOCK_DATABASE_PASSWORD, STOCK_DATABASE


def dumpDatabase():
    initLogger()

    server_ip = MONGODB_SERVER_IP
    server_port = MONGODB_SERVER_PORT
    username = STOCK_DATABASE_USERNAME
    password = STOCK_DATABASE_PASSWORD
    database_name = STOCK_DATABASE

    client = MongoClient(server_ip,
                         server_port,
                         username=username,
                         password=password,
                         authSource=database_name)

    db = client[database_name]
    collections = db.list_collection_names()
    result = ''
    result += '\nDatabase: {}\nCollections: {}\n'.format(db.name, collections)
    for c in collections:
        result += 'Collection {}:\n'.format(c)
        indexes = db[c].list_indexes()
        for i in indexes:
            result += 'Index: {}\n'.format(i)
    logging.info(result)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='数据库管理')
    parser.add_argument('--dump_database', action='store_true', default=False,
                        help='显示数据库信息')
    args = parser.parse_args()

    initDefaultExceptionHandler()

    if args.dump_database:
        dumpDatabase()
