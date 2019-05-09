# coding: utf-8
'''利润表
https://tushare.pro/document/2?doc_id=33
'''

import time
import logging
from datetime import datetime
from .base_collector import TushareMongodbBaseCollector
from .stock_basic import StockBasicCollector
from ..utils import singleton, getQuarterOfYears, getPeriodOfYears


@singleton
class IncomeCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='income',
            primary_key={'ts_code': 1, 'report_type': 1, 'end_date': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    def getIncome(self, ts_code, start_date=None, end_date=None,
                  report_type=None):
        tushare = self.getTushare()
        data = tushare.income(
            ts_code=ts_code,
            ann_date='',
            start_date=start_date,
            end_date=end_date,
            period='',
            report_type=report_type,
            comp_type='',
            fields=
            'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit'
        )
        return data.to_dict(orient='records')

    # 不替换旧记录
    def compareRecord(self, new_record, old_record):
        return 0

    def update(self):
        logging.info('update 利润表(income) ...')

        ts_codes = StockBasicCollector().getStockCodes()
        self.openCollection()
        inserted_total = replaced_total = checked_total = 0
        for ts_code in ts_codes:
            current_year = datetime.now().year

            stock_quarters = set()
            for doc in self.collection.find({'ts_code': ts_code, 'report_type': '1'},
                                            {'end_date': 1}):
                stock_quarters.add(doc['end_date'])

            max_year = None
            try:
                max_year = int(max(stock_quarters)[:4])
            except ValueError:
                pass
            max_year = max_year if max_year else current_year - 20

            all_quarters = getQuarterOfYears(max_year, current_year)

            t = all_quarters - stock_quarters
            if t:
                min_year = int(min(t)[:4])
                start_date, end_date = getPeriodOfYears(min_year, current_year)
            else:
                start_date, end_date = getPeriodOfYears(current_year, current_year)

            logging.debug('get {}-{} income of {} from tushare'.format(
                start_date, end_date, ts_code))

            data = None
            delay_time = 61
            while True:
                try:
                    data = self.getIncome(ts_code, start_date, end_date, report_type='1')
                    break
                except Exception as e:
                    logging.debug(e)
                    time.sleep(delay_time)
                    delay_time += 30

            if not data:
                continue

            data = sorted(data, key=lambda x: x['end_date'])
            inserted_count, replaced_count = self.updateCollection(data)

            checked_total += len(data)
            inserted_total += inserted_count
            replaced_total += replaced_count

        logging.info(
            'update income finished: {} checked, {} inserted, {} replaced'
            .format(checked_total, inserted_total, replaced_total))
