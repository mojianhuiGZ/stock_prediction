# coding: utf-8

import gevent
from gevent import monkey
monkey.patch_all()

import time
import logging
import argparse
from pymongo import MongoClient
from stock.utils import initLogger, initDefaultExceptionHandler
from settings import TUSHARE_TOKEN, MONGODB_SERVER_IP, MONGODB_SERVER_PORT, \
    STOCK_DATABASE_USERNAME, STOCK_DATABASE_PASSWORD, STOCK_DATABASE
from stock.collectors.stock_basic import StockBasicCollector
from stock.collectors.stock_company import StockCompanyCollector
from stock.collectors.income import IncomeCollector
from stock.collectors.balancesheet import BalancesheetCollector
from stock.collectors.cashflow import CashflowCollector
from stock.collectors.daily_basic import DailyBasicCollector
from stock.collectors.dividend import DividendCollector
from stock.collectors.daily import DailyCollector
from stock.collectors.trade_cal import TradeCalCollector
from stock.collectors.fina_indicator import FinaIndicatorCollector
from stock.collectors.index_basic import IndexBasicCollector
from stock.collectors.index_daily import IndexDailyCollector


def getCollector(collector_class):
    collector = collector_class(token=TUSHARE_TOKEN,
                                server_ip=MONGODB_SERVER_IP,
                                server_port=MONGODB_SERVER_PORT,
                                username=STOCK_DATABASE_USERNAME,
                                password=STOCK_DATABASE_PASSWORD,
                                database_name=STOCK_DATABASE)
    return collector


if __name__ == '__main__':
    #parser = argparse.ArgumentParser(description='更新股票数据')
    #parser.add_argument('--stock_basic', action='store_true', default=False,
                        #help='更新股市基本信息')
    #parser.add_argument('--stock_basic', action='store_true', default=False,
                        #help='更新上市公司财务数据')
    #parser.add_argument('--stock_basic', action='store_true', default=False,
                        #help='更新股市每日数据')
    #parser.add_argument('--company', action='store_true', default=False,
                        #help='更新上市公司数据')
    #parser.add_argument('--update_index', action='store_true', default=False,
                        #help='更新指数数据')
    #args = parser.parse_args()

    initLogger()
    initDefaultExceptionHandler()
    logging.info('***********************************************************')
    logging.info('update')

    threads = []

    if args.update_company:
        trade_cal_collector = getCollector(TradeCalCollector)
        trade_cal_collector.update()

        stock_basic_collector = getCollector(StockBasicCollector)
        stock_company_collector = getCollector(StockCompanyCollector)

        income_collector = getCollector(IncomeCollector)
        balance_sheet_collector = getCollector(BalancesheetCollector)
        cashflow_collector = getCollector(CashflowCollector)
        dividend_collector = getCollector(DividendCollector)
        fina_indicator_collector = getCollector(FinaIndicatorCollector)

        daily_basic_collector = getCollector(DailyBasicCollector)
        daily_collector = getCollector(DailyCollector)

        stock_basic_collector.update()
        stock_company_collector.update()

        threads += [
            gevent.spawn(income_collector.update),
            gevent.spawn(balance_sheet_collector.update),
            gevent.spawn(cashflow_collector.update),
            gevent.spawn(fina_indicator_collector.update),
            gevent.spawn(dividend_collector.update),
        ]

        threads += [
            gevent.spawn(daily_basic_collector.update),
            gevent.spawn(daily_collector.update),
        ]

    if args.update_index:
        index_basic_collector = getCollector(IndexBasicCollector)
        index_basic_collector.update()

        index_daily_collector = getCollector(IndexDailyCollector)

        threads += [
            gevent.spawn(IndexDailyCollector.update),
        ]

    while threads:
        for thread in threads:
            if thread.successful():
                print('thread {} successful return value {}'.format(
                    thread, thread.value))
                threads.remove(thread)
            elif thread.dead:
                print('thread {} dead'.format(thread))
                thread.get()
                threads.remove(thread)

        time.sleep(0.5)

