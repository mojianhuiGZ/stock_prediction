# coding: utf-8

import logging
from utils import initializeLogger, initializeDefaultExceptionHandler
from settings import TUSHARE_TOKEN, MONGODB_SERVER_IP, MONGODB_SERVER_PORT, \
    STOCK_DATABASE_USERNAME, STOCK_DATABASE_PASSWORD, STOCK_DATABASE
from collectors.stock_basic import StockBasicCollector
from collectors.stock_company import StockCompanyCollector
from collectors.income import IncomeCollector


def getCollector(collector_class):
    collector = collector_class(token=TUSHARE_TOKEN,
                                server_ip=MONGODB_SERVER_IP,
                                server_port=MONGODB_SERVER_PORT,
                                username=STOCK_DATABASE_USERNAME,
                                password=STOCK_DATABASE_PASSWORD,
                                database_name=STOCK_DATABASE)
    return collector


if __name__ == '__main__':
    initializeLogger()
    initializeDefaultExceptionHandler()
    logging.info('***********************************************************')

    collector = getCollector(StockBasicCollector)
    collector.update()
    collector = getCollector(StockCompanyCollector)
    collector.update()
    collector = getCollector(IncomeCollector)
    collector.update()
