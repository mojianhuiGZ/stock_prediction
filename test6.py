# coding: utf-8
from pymongo import MongoClient
client = MongoClient('127.0.0.1', 27017, username='mjh', password='666666', authSource='stock')
db = client['stock']
db.list_collection_names()
collection = db['stock_basic']
collection.find_one({})
collection.find_one({'ts_code': '000003.SZ'})
collection.find_one({'ts_code': '000004.SZ'})
from pandas import DataFrame
df = DataFrame([{
 'ts_code': '000001.SZ',
 'symbol': '000001',
 'name': '平安银行',
 'area': '深圳',
 'industry': '银行',
 'fullname': '平安银行股份有限公司',
 'enname': 'Ping An Bank Co., Ltd.',
 'market': '主板',
 'exchange': 'SZSE',
 'curr_type': 'CNY',
 'list_status': 'L',
 'list_date': '19910403',
 'delist_date': None,
 'is_hs': 'S'},
 {
 'ts_code': '000004.SZ',
 'symbol': '000004',
 'name': '国农科技',
 'area': '深圳',
 'industry': '生物制药',
 'fullname': '深圳中国农大科技股份有限公司',
 'enname': 'Shenzhen Cau Technology Co.,Ltd.',
 'market': '主板',
 'exchange': 'SZSE',
 'curr_type': 'CNY',
 'list_status': 'L',
 'list_date': '19910114',
 'delist_date': None,
 'is_hs': 'N'},
 ])
df
df = DataFrame([{
 'ts_code': '000001.SZ',
 'symbol': '000001',
 'name': '平安银行',
 'area': '深圳',
 'industry': '银行',
 'fullname': '平安银行股份有限公司',
 'enname': 'Ping An Bank Co., Ltd.',
 'market': '主板',
 'exchange': 'SZSE',
 'curr_type': 'CNY',
 'list_status': 'L',
 'list_date': '19910403',
 'delist_date': None,
 'is_hs': 'S'},
 {
 'ts_code': '000004.SZ',
 'symbol': '000004',
 'name': '国农科技',
 'area': '深圳',
 'industry': '生物制药',
 'fullname': '深圳中国农大科技股份有限公司',
 'enname': 'Shenzhen Cau Technology Co.,Ltd.',
 'market': '主板',
 'exchange': 'SZSE',
 'curr_type': 'CNY',
 'list_status': 'L',
 'list_date': '19910114',
 'delist_date': None,
 'is_hs': 'N'},
 ], columns=['ts_code','symbol','name','area','industry','fullname','enname','market','exchange','curr_type','list_status','list_date','delist_date','is_hs'])
df
df = DataFrame([{
 'ts_code': '000001.SZ',
 'symbol': '000001',
 'name': '平安银行',
 'area': '深圳',
 'industry': '银行',
 'fullname': '平安银行股份有限公司',
 'enname': 'Ping An Bank Co., Ltd.',
 'market': '主板',
 'exchange': 'SZSE',
 'curr_type': 'CNY',
 'list_status': 'L',
 'list_date': '19910403',
 'delist_date': None,
 'is_hs': 'S'},
 {
 'ts_code': '000004.SZ',
 'symbol': '000004',
 'name': '国农科技',
 'area': '深圳',
 'industry': '生物制药',
 'fullname': '深圳中国农大科技股份有限公司',
 'enname': 'Shenzhen Cau Technology Co.,Ltd.',
 'market': '主板',
 'exchange': 'SZSE',
 'curr_type': 'CNY',
 'list_status': 'L',
 'list_date': '19910114',
 'delist_date': None,
 'is_hs': 'N'},
 ], columns=['ts_code','symbol1111','name','area','industry','fullname','enname','market','exchange','curr_type','list_status','list_date','delist_date','is_hs'])
df
df = DataFrame([{
 'ts_code': '000001.SZ',
 'symbol': '000001',
 'name': '平安银行',
 'area': '深圳',
 'industry': '银行',
 'fullname': '平安银行股份有限公司',
 'enname': 'Ping An Bank Co., Ltd.',
 'market': '主板',
 'exchange': 'SZSE',
 'curr_type': 'CNY',
 'list_status': 'L',
 'list_date': '19910403',
 'delist_date': None,
 'is_hs': 'S'},
 {
 'ts_code': '000004.SZ',
 'symbol': '000004',
 'name': '国农科技',
 'area': '深圳',
 'industry': '生物制药',
 'fullname': '深圳中国农大科技股份有限公司',
 'enname': 'Shenzhen Cau Technology Co.,Ltd.',
 'market': '主板',
 'exchange': 'SZSE',
 'curr_type': 'CNY',
 'list_status': 'L',
 'list_date': '19910114',
 'delist_date': None,
 'is_hs': 'N'},
 ], columns=['ts_code','symbol','name','area','industry','fullname','enname','market','exchange','curr_type','list_status','list_date','delist_date','is_hs'])
df
get_ipython().system('ls')
get_ipython().run_line_magic('save', 'test6 1-1000')
