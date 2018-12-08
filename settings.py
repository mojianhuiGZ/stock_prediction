# coding: utf-8

import os
import logging

LOG_FILE = os.path.join(os.getcwd(), 'stock.log')
LOG_PATH = os.getcwd()
# LOG_LEVEL = logging.INFO
LOG_LEVEL = logging.DEBUG

TUSHARE_TOKEN = '75f48b9c2d4499ef1e37b013ad7acd1583c63afcefababba16417e3a'
MONGODB_SERVER_IP = 'localhost'
MONGODB_SERVER_PORT = 27017
STOCK_DATABASE = 'stock'
STOCK_DATABASE_USERNAME = 'mjh'
STOCK_DATABASE_PASSWORD = '666666'
