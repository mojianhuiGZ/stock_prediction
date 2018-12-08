# coding: utf-8
'''利润表
https://tushare.pro/document/2?doc_id=33
'''

import time
import logging
from .collectors import TushareMongodbBaseCollector
from .stock_basic import StockBasicCollector


class IncomeCollector(TushareMongodbBaseCollector):
    collection_name = 'income'

    def __init__(self, token, server_ip, server_port, username, password,
                 database_name):
        super().__init__(token, server_ip, server_port, username, password,
                         database_name,
                         primary_key={'ts_code': 1, 'end_date': 1, 'report_type': 1},
                         insert_update_time=True,
                         log_collection_operation=True)
        self.ts_codes = None
        self.validator = None

    def getTsCodes(self):
        collection = self.openCollection(StockBasicCollector.collection_name)
        self.ts_codes = {}
        for doc in collection.find({}, {'ts_code': True, '_id': False}):
            self.ts_codes[doc['ts_code']] = 0

        collection = self.openCollection(IncomeCollector.collection_name)
        for doc in collection.find({}):
            self.ts_codes[doc['ts_code']] += 1
        t = sorted(self.ts_codes.items(), key=lambda item: item[1],)
        t = [i[0] for i in t]

        self.ts_codes = t
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

    def compareRecord(self, new_record, old_record, with_update_time=False):
        old_record = {i[0]:old_record[i[0]] for i in self._primary_key.items()}
        new_record = {i[0]:new_record[i[0]] for i in self._primary_key.items()}
        return True if set(old_record.items()) ^ set(new_record.items()) else False

    def update(self):
        logging.info('update 利润表(income) ...')

        logging.info('open mongodb database ...')
        logging.info('mongodb server ip and port: {}:{}'.format(
            self.getServerIP(), self.getServerPort()))
        logging.info('mongodb database and collection: {}.{}'.format(
            self.getDatabaseName(), IncomeCollector.collection_name))

        self.getTsCodes()

        collection = self.openCollection(IncomeCollector.collection_name,
                                         self.validator)

        inserted_total = replaced_total = checked_total = 0
        for ts_code in self.ts_codes:
            logging.debug('get income({}) from tushare'.format(ts_code))

            data = None
            delay_time = 60
            while not data:
                try:
                    data = self.getIncome(ts_code)
                except Exception as e:
                    logging.debug(e)
                    time.sleep(delay_time)
                    delay_time += 30

            logging.debug('update to mongodb database')

            insert_count, replace_count = self.updateCollection(data)

            checked_total += len(data)
            inserted_total += insert_count
            replaced_total += replace_count

        logging.info('get income from tushare ...')
        data = self.getIncome()

        self.openDatabase(IncomeCollector.collection_name, self.validator)

        logging.info('update to mongodb database ...')

        insert_count, replace_count = self.updateDatabase(data)

        logging.info(
            'update income finished: {} checked, {} inserted, {} replaced'.
            format(len(data), insert_count, replace_count))
