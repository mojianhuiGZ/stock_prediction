# coding: utf-8

'''{{ title }}
{{ description }}
'''

import logging
from .collectors import TushareMongodbBaseCollector


class {{ class_name | camel }}Collector(TushareMongodbBaseCollector):
    collection_name = '{{ collection_name }}'

    def __init__(self, token, server_ip, server_port,
                 username, password, database_name):
        super().__init__(token, server_ip, server_port,
                         username, password, database_name,
                         primary_key=None, # TODO
                         insert_update_time=True,
                         log_collection_operation=True)
        self.validator = {{ validator }}

    def get{{ api_name | camel }}(self):
        tushare = self.getTushare()
        data = tushare.{{ query_command }}
        return data.to_dict(orient='records')

    def update(self):
        logging.info('update {{ title }}({{ api_name }}) ...')

        logging.info('get {{ api_name }} from tushare ...')
        data = self.get{{ api_name | camel }}()

        logging.info('open mongodb database ...')
        logging.info('mongodb server ip and port: {}:{}'.format(
            self.getServerIP(), self.getServerPort()))
        logging.info('mongodb database and collection: {}.{}'.format(
            self.getDatabaseName(), {{ class_name | camel }}Collector.collection_name))

        self.openCollection({{ class_name | camel }}Collector.collection_name, self.validator)

        logging.info('update to mongodb database ...')

        insert_count, replace_count = self.updateCollection(data)

        logging.info('update {{ collection_name }} finished: {} checked, {} inserted, {} replaced'.format(
                         len(data), insert_count, replace_count))
