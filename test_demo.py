# coding: utf-8

import time
from collectors.basic_info import BasicInfoCollector
from utils import initializeLogger
from settings import TUSHARE_TOKEN, MONGODB_SERVER_IP, MONGODB_SERVER_PORT, \
    STOCK_DATABASE_USERNAME, STOCK_DATABASE_PASSWORD, STOCK_DATABASE


def test_basic_info_update(monkeypatch):
    initializeLogger()

    client = BasicInfoCollector(token=TUSHARE_TOKEN,
                                server_ip=MONGODB_SERVER_IP,
                                server_port=MONGODB_SERVER_PORT,
                                username=STOCK_DATABASE_USERNAME,
                                password=STOCK_DATABASE_PASSWORD,
                                database_name=STOCK_DATABASE,
                                collection_name='test')
    collection = client.openDatabase(client.collection_name, client.validator)
    collection.delete_many({})

    data = [{'ts_code': '603825.SH', 'symbol': '603825', 'name': '华扬联众', 'area': '北京', 'industry': '互联网', 'fullname': '华扬联众数字技术股份有限公司', 'enname': 'Hylink Digital Solution Co., Ltd.', 'market': '主板', 'exchange': 'SSE', 'curr_type': 'CNY', 'list_status': 'L', 'list_date': '20170802', 'delist_date': None, 'is_hs': 'N'},
            {'ts_code': '603909.SH', 'symbol': '603909', 'name': '合诚股份', 'area': '福建', 'industry': '建筑施工', 'fullname': '合诚工程咨询集团股份有限公司', 'enname': 'Holsin Engineering Consulting Group Co.,Ltd.', 'market': '主板', 'exchange': 'SSE', 'curr_type': 'CNY', 'list_status': 'L', 'list_date': '20160628', 'delist_date': None, 'is_hs': 'N'}]

    inserted_count, _, _ = client.updateDatabase(data, collection)
    assert inserted_count == 2

    time.sleep(1)

    data[0]['name'] = 'ST华扬联众'
    _, replaced_count, _ = client.updateDatabase(data, collection)
    assert replaced_count == 1

    time.sleep(1)

    data = [data[0]]
    _, _, deleted_count = client.updateDatabase(data, collection)
    assert deleted_count == 1

    collection.drop()
