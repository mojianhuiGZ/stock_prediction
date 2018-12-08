# coding: utf-8

import os
import re
import argparse
from bs4 import BeautifulSoup
from urllib.request import urlopen
from jinja2 import Environment, PackageLoader


def camel(name):
    result = ''
    for s in name.lower().split('_'):
        result = result + s[0].upper() + s[1:]
    return result


def get_file_size(file_path):
    fsize = 0
    try:
        fsize = os.path.getsize(file_path.encode('utf8'))
    except FileNotFoundError:
        pass
    return fsize


def generate_collector(url, output_file):
    if get_file_size(output_file) > 0:
        overwrite = input('要覆盖{}的内容吗[y/n]? '.format(output_file))
        overwrite = True if overwrite == 'y' or overwrite == 'Y' else False
        if not overwrite:
            return

    response = urlopen(url)
    content = response.read()
    document = BeautifulSoup(content, 'lxml')

    title = document.select('.content h2')
    title = title[0].text

    descriptions = document.select('.content p')
    api_description = descriptions[0]
    api = re.search(r'接口：\s*([a-zA-Z-_]+)', api_description.text)[1]

    tables = document.select('.content table')
    table_input = tables[0]
    table_output = tables[1]

    n = len(table_input.select('th'))
    query_command = api + '('
    for index, tag in enumerate(table_input.select('td')):
        if index % n == 0:
            query_command += tag.text + "='', "
    query_command += "fields='"
    n = len(table_output.select('th'))
    for index, tag in enumerate(table_output.select('td')):
        if index % n == 0:
            query_command += tag.text + ','
    query_command = query_command.strip(',')
    query_command += "')"

    validator = {'$jsonSchema': {'bsonType': 'object', 'required': [], 'properties': {}}}

    n = len(table_output.select('th'))
    for index, tag in enumerate(table_output.select('td')):
        if index % n == 0:
            validator['$jsonSchema']['required'].append(tag.text)
    validator['$jsonSchema']['required'].append('update_time')

    t = []
    for tag in table_output.select('td'):
        t.append(tag.text)
    t = [t[i:i+n] for i in range(0, len(t), n)]
    for item in t:
        if item[1] == 'str':
            type_name = 'string'
        elif item[1] == 'float':
            type_name = 'double'
        elif item[1] == 'int':
            type_name = 'int'
        validator['$jsonSchema']['properties'][item[0]] = \
            {'bsonType': type_name, 'title': item[-1]}
    validator['$jsonSchema']['properties']['update_time'] = \
        {'bsonType': 'date', 'title': '更新日期'}

    env = Environment(loader=PackageLoader('collectors', 'templates'))
    env.filters['camel'] = camel
    t = env.get_template('collector.py')
    result = t.render(title=title,
                      description=url,
                      class_name=api,
                      collection_name=api,
                      api_name=api,
                      query_command=query_command,
                      validator=validator)

    with open(output_file, 'w') as f:
        f.write(result)
        f.close()

    os.system('yapf --in-place {}'.format(output_file))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='采集器自动生成')
    parser.add_argument('api_url', help='tushare pro数据接口URL')
    parser.add_argument('output_file', help='输出文件')
    args = parser.parse_args()
    generate_collector(args.api_url, args.output_file)
