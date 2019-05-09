# coding: utf-8

from stock.collectors.stock_basic import StockBasicCollector
from stock.collectors.stock_company import StockCompanyCollector


def test_stock_basic_collector():
    print('TEST StockBasicCollector.dump:')
    StockBasicCollector().dump()

    print('TEST StockBasicCollector.getStockCodes:')
    codes = StockBasicCollector().getStockCodes()
    print(len(codes))

    print('TEST StockBasicCollector.update:')
    StockBasicCollector().update()


def test_stock_company_collector():
    print('TEST StockCompanyCollector:')
    StockCompanyCollector().dump()
    StockCompanyCollector().update()


if __name__ == '__main__':
    test_stock_basic_collector()
    test_stock_company_collector()
