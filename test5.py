# coding: utf-8
import tushare as ts
ts.set_token('75f48b9c2d4499ef1e37b013ad7acd1583c63afcefababba16417e3a')
pro = ts.pro_api()
import pymongo
get_ipython().system('cat collectors/collectors.py')
from pymongo import MongoClient
client = MongoClient('127.0.0.1', '27017', username='mjh', password='666666', authSource='stock')  
client = MongoClient('127.0.0.1', 27017, username='mjh', password='666666', authSource='stock')  
db = client['stock']
db.list_collection_names()
collection = db['income']
collection.find_one({})
collection.find_one({}, {'end_date': 1})
collection.find_one({}, {'end_date': 1, '_id': 0})
{v for v in collection.find_one({}, {'end_date': 1, '_id': 0})}
dates = {}
dates = {}
{v[0] for v in collection.find_one({}, {'end_date': 1, '_id': 0})}
{v.items() for v in collection.find_one({}, {'end_date': 1, '_id': 0})}
for v in collection.find_one({}, {'end_date': 1, '_id': 0}):
    print v
for v in collection.find_one({}, {'end_date': 1, '_id': 0}):
    print(v)
    
for v in collection.find({}, {'end_date': 1, '_id': 0}):
    print(v)
    
{v['end_date']: 1 for v in collection.find({}, {'end_date': 1, '_id': 0})}

    
db.list_collection_names()
collection = db['balancesheet']
{v['end_date']: 1 for v in collection.find({}, {'end_date': 1, '_id': 0})}
collection = db['cashflow']
{v['end_date']: 1 for v in collection.find({}, {'end_date': 1, '_id': 0})}
collection = db['dividend']
{v['end_date']: 1 for v in collection.find({}, {'end_date': 1, '_id': 0})}
a = {v['end_date']: 1 for v in collection.find({}, {'end_date': 1, '_id': 0})}
sorted(a.items())
get_ipython().system('df -h')
get_ipython().system('df ')
get_ipython().system('df ')
get_ipython().system('df ')
pro.trade_cal()
pro.trade_cal(exchange='SZSE')
get_ipython().run_line_magic('save', 'test5 1-1000')
