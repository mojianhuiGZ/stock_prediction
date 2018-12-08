# coding: utf-8
'''利润表
https://tushare.pro/document/2?doc_id=33
'''

import time
import logging
from random import shuffle
from .collectors import TushareMongodbBaseCollector
from .stock_basic import StockBasicCollector


class IncomeCollector(TushareMongodbBaseCollector):
    collection_name = 'income'

    def __init__(self, token, server_ip, server_port, username, password,
                 database_name):
        super().__init__(token, server_ip, server_port, username, password,
                         database_name)
        self.ts_codes = None
        self.validator = {
            '$jsonSchema': {
                'bsonType':
                'object',
                'required': [
                    'ts_code', 'ann_date', 'f_ann_date', 'end_date',
                    'report_type', 'comp_type', 'basic_eps', 'diluted_eps',
                    'total_revenue', 'revenue', 'int_income', 'prem_earned',
                    'comm_income', 'n_commis_income', 'n_oth_income',
                    'n_oth_b_income', 'prem_income', 'out_prem',
                    'une_prem_reser', 'reins_income', 'n_sec_tb_income',
                    'n_sec_uw_income', 'n_asset_mg_income', 'oth_b_income',
                    'fv_value_chg_gain', 'invest_income', 'ass_invest_income',
                    'forex_gain', 'total_cogs', 'oper_cost', 'int_exp',
                    'comm_exp', 'biz_tax_surchg', 'sell_exp', 'admin_exp',
                    'fin_exp', 'assets_impair_loss', 'prem_refund',
                    'compens_payout', 'reser_insur_liab', 'div_payt',
                    'reins_exp', 'oper_exp', 'compens_payout_refu',
                    'insur_reser_refu', 'reins_cost_refund', 'other_bus_cost',
                    'operate_profit', 'non_oper_income', 'non_oper_exp',
                    'nca_disploss', 'total_profit', 'income_tax', 'n_income',
                    'n_income_attr_p', 'minority_gain', 'oth_compr_income',
                    't_compr_income', 'compr_inc_attr_p', 'compr_inc_attr_m_s',
                    'ebit', 'ebitda', 'insurance_exp', 'undist_profit',
                    'distable_profit', 'update_time'
                ],
                'properties': {
                    'ts_code': {
                        'bsonType': 'string',
                        'title': 'TS股票代码'
                    },
                    'ann_date': {
                        'bsonType': ['string', 'null'],
                        'title': '公告日期'
                    },
                    'f_ann_date': {
                        'bsonType': ['string', 'null'],
                        'title': '实际公告日期'
                    },
                    'end_date': {
                        'bsonType': 'string',
                        'title': '报告期'
                    },
                    'report_type': {
                        'bsonType': 'string',
                        'title': '报告类型'
                    },
                    'comp_type': {
                        'bsonType': ['string', 'null'],
                        'title': '公司类型'
                    },
                    'basic_eps': {
                        'bsonType': 'double',
                        'title': '基本每股收益'
                    },
                    'diluted_eps': {
                        'bsonType': ['double', 'null'],
                        'title': '稀释每股收益'
                    },
                    'total_revenue': {
                        'bsonType': 'double',
                        'title': '营业总收入'
                    },
                    'revenue': {
                        'bsonType': 'double',
                        'title': '营业收入'
                    },
                    'int_income': {
                        'bsonType': ['double', 'null'],
                        'title': '利息收入'
                    },
                    'prem_earned': {
                        'bsonType': ['double', 'null'],
                        'title': '已赚保费'
                    },
                    'comm_income': {
                        'bsonType': ['double', 'null'],
                        'title': '手续费及佣金收入'
                    },
                    'n_commis_income': {
                        'bsonType': ['double', 'null'],
                        'title': '手续费及佣金净收入'
                    },
                    'n_oth_income': {
                        'bsonType': ['double', 'null'],
                        'title': '其他经营净收益'
                    },
                    'n_oth_b_income': {
                        'bsonType': ['double', 'null'],
                        'title': '加:其他业务净收益'
                    },
                    'prem_income': {
                        'bsonType': ['double', 'null'],
                        'title': '保险业务收入'
                    },
                    'out_prem': {
                        'bsonType': ['double', 'null'],
                        'title': '减:分出保费'
                    },
                    'une_prem_reser': {
                        'bsonType': ['double', 'null'],
                        'title': '提取未到期责任准备金'
                    },
                    'reins_income': {
                        'bsonType': ['double', 'null'],
                        'title': '其中:分保费收入'
                    },
                    'n_sec_tb_income': {
                        'bsonType': ['double', 'null'],
                        'title': '代理买卖证券业务净收入'
                    },
                    'n_sec_uw_income': {
                        'bsonType': ['double', 'null'],
                        'title': '证券承销业务净收入'
                    },
                    'n_asset_mg_income': {
                        'bsonType': ['double', 'null'],
                        'title': '受托客户资产管理业务净收入'
                    },
                    'oth_b_income': {
                        'bsonType': ['double', 'null'],
                        'title': '其他业务收入'
                    },
                    'fv_value_chg_gain': {
                        'bsonType': ['double', 'null'],
                        'title': '加:公允价值变动净收益'
                    },
                    'invest_income': {
                        'bsonType': ['double', 'null'],
                        'title': '加:投资净收益'
                    },
                    'ass_invest_income': {
                        'bsonType': ['double', 'null'],
                        'title': '其中:对联营企业和合营企业的投资收益'
                    },
                    'forex_gain': {
                        'bsonType': ['double', 'null'],
                        'title': '加:汇兑净收益'
                    },
                    'total_cogs': {
                        'bsonType': 'double',
                        'title': '营业总成本'
                    },
                    'oper_cost': {
                        'bsonType': ['double', 'null'],
                        'title': '减:营业成本'
                    },
                    'int_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '减:利息支出'
                    },
                    'comm_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '减:手续费及佣金支出'
                    },
                    'biz_tax_surchg': {
                        'bsonType': ['double', 'null'],
                        'title': '减:营业税金及附加'
                    },
                    'sell_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '减:销售费用'
                    },
                    'admin_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '减:管理费用'
                    },
                    'fin_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '减:财务费用'
                    },
                    'assets_impair_loss': {
                        'bsonType': ['double', 'null'],
                        'title': '减:资产减值损失'
                    },
                    'prem_refund': {
                        'bsonType': ['double', 'null'],
                        'title': '退保金'
                    },
                    'compens_payout': {
                        'bsonType': ['double', 'null'],
                        'title': '赔付总支出'
                    },
                    'reser_insur_liab': {
                        'bsonType': ['double', 'null'],
                        'title': '提取保险责任准备金'
                    },
                    'div_payt': {
                        'bsonType': ['double', 'null'],
                        'title': '保户红利支出'
                    },
                    'reins_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '分保费用'
                    },
                    'oper_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '营业支出'
                    },
                    'compens_payout_refu': {
                        'bsonType': ['double', 'null'],
                        'title': '减:摊回赔付支出'
                    },
                    'insur_reser_refu': {
                        'bsonType': ['double', 'null'],
                        'title': '减:摊回保险责任准备金'
                    },
                    'reins_cost_refund': {
                        'bsonType': ['double', 'null'],
                        'title': '减:摊回分保费用'
                    },
                    'other_bus_cost': {
                        'bsonType': ['double', 'null'],
                        'title': '其他业务成本'
                    },
                    'operate_profit': {
                        'bsonType': 'double',
                        'title': '营业利润'
                    },
                    'non_oper_income': {
                        'bsonType': ['double', 'null'],
                        'title': '加:营业外收入'
                    },
                    'non_oper_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '减:营业外支出'
                    },
                    'nca_disploss': {
                        'bsonType': ['double', 'null'],
                        'title': '其中:减:非流动资产处置净损失'
                    },
                    'total_profit': {
                        'bsonType': 'double',
                        'title': '利润总额'
                    },
                    'income_tax': {
                        'bsonType': ['double', 'null'],
                        'title': '所得税费用'
                    },
                    'n_income': {
                        'bsonType': 'double',
                        'title': '净利润(含少数股东损益)'
                    },
                    'n_income_attr_p': {
                        'bsonType': 'double',
                        'title': '净利润(不含少数股东损益)'
                    },
                    'minority_gain': {
                        'bsonType': ['double', 'null'],
                        'title': '少数股东损益'
                    },
                    'oth_compr_income': {
                        'bsonType': ['double', 'null'],
                        'title': '其他综合收益'
                    },
                    't_compr_income': {
                        'bsonType': 'double',
                        'title': '综合收益总额'
                    },
                    'compr_inc_attr_p': {
                        'bsonType': 'double',
                        'title': '归属于母公司(或股东)的综合收益总额'
                    },
                    'compr_inc_attr_m_s': {
                        'bsonType': ['double', 'null'],
                        'title': '归属于少数股东的综合收益总额'
                    },
                    'ebit': {
                        'bsonType': 'double',
                        'title': '息税前利润'
                    },
                    'ebitda': {
                        'bsonType': 'double',
                        'title': '息税折旧摊销前利润'
                    },
                    'insurance_exp': {
                        'bsonType': ['double', 'null'],
                        'title': '保险业务支出'
                    },
                    'undist_profit': {
                        'bsonType': ['double', 'null'],
                        'title': '年初未分配利润'
                    },
                    'distable_profit': {
                        'bsonType': ['double', 'null'],
                        'title': '可分配利润'
                    },
                    'update_time': {
                        'bsonType': 'date',
                        'title': '更新日期'
                    }
                }
            }
        }

    def getTsCodes(self):
        collection = self.openDatabase(StockBasicCollector.collection_name,
                                       create=False)
        self.ts_codes = []
        for doc in collection.find({}, {'ts_code': True, '_id': False}):
            self.ts_codes.append(doc['ts_code'])
        shuffle(self.ts_codes)
        return self.ts_codes

    def getIncome(self, ts_code):
        tushare = self.getTushare()
        data = tushare.income(
            ts_code=ts_code,
            ann_date='',
            start_date='',
            end_date='',
            period='',
            report_type='',
            comp_type='',
            fields=
            'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit'
        )
        return data.to_dict(orient='records')

    def update(self):
        logging.info('update 利润表(income) ...')

        logging.info('open mongodb database')
        logging.info('mongodb server ip and port: {}:{}'.format(
            self.getServerIP(), self.getServerPort()))
        logging.info('mongodb database and collection: {}.{}'.format(
            self.getDatabaseName(), IncomeCollector.collection_name))

        collection = self.openDatabase(IncomeCollector.collection_name,
                                       self.validator, drop_old=True)

        self.getTsCodes()

        inserted_total = replaced_total = deleted_total = checked_total = 0
        for ts_code in self.ts_codes:
            logging.debug('get income({}) from tushare'.format(ts_code))

            data = None
            while not data:
                try:
                    data = self.getIncome(ts_code)
                except Exception as e:
                    logging.debug(e)
                    time.sleep(1)

            logging.debug('update to mongodb database')

            insert_count, replace_count, delete_count = self.updateDatabase(
                data, collection)

            checked_total += len(data)
            inserted_total += insert_count
            replaced_total += replace_count
            deleted_total += delete_count

            time.sleep(1)

        logging.info(
            'update income finished: {} checked {} inserted {} replaced {} deleted!'
            .format(checked_total, inserted_total, replaced_total, deleted_total))
