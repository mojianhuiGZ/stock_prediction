# coding: utf-8
from stock.collectors.stock_basic import StockBasicCollector
StockBasicCollector().dump()
c = StockBasicCollector()
c.getStockCodes()
c.update()
