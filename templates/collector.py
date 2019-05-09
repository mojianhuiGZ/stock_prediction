# coding: utf-8

'''{{ title }}
{{ description }}
'''

import logging
from .collectors import TushareMongodbBaseCollector
from .utils import singleton


@singleton
class {{ class_name | camel }}Collector(TushareMongodbBaseCollector):

    def __init__(self, token, server_ip, server_port,
                 username, password, database_name):
        super().__init__(token, server_ip, server_port,
                         username, password, database_name,
                         primary_key=None, # TODO
                         collection_name = None, # TODO
                         get_ts_code_from=None, # TODO
                         get_open_days_from=None, # TODO
                         )
        self.validator = {{ validator }}

    def get{{ api_name | camel }}(self):
        tushare = self.getTushare()
        data = tushare.{{ query_command }}
        return data.to_dict(orient='records')

    def update(self):
        logging.info('update {{ title }}({{ api_name }}) ...')

        data = self.get{{ api_name | camel }}()

        self.openCollection(self.getCollectionName(), self.validator)

        insert_count, replace_count = self.updateCollection(data)

        logging.info('update {{ collection_name }} finished: {} checked, {} inserted, {} replaced'.format(
                         len(data), insert_count, replace_count))
