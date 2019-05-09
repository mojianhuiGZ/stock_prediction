# coding: utf-8
'''资产负债表
https://tushare.pro/document/2?doc_id=36
'''

import time
import logging
from datetime import datetime
from .base_collector import TushareMongodbBaseCollector
from .stock_basic import StockBasicCollector
from ..utils import singleton, getQuarterOfYears, getPeriodOfYears


@singleton
class BalancesheetCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='balancesheet',
            primary_key={'ts_code': 1, 'report_type': 1, 'end_date': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    def getBalancesheet(self, ts_code, start_date=None, end_date=None,
                        report_type=None):
        tushare = self.getTushare()
        data = tushare.balancesheet(
            ts_code=ts_code,
            ann_date='',
            start_date=start_date,
            end_date=end_date,
            period='',
            report_type=report_type,
            comp_type='',
            fields=
            'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales'
        )
        return data.to_dict(orient='records')

    # 不替换旧记录
    def compareRecord(self, new_record, old_record):
        return 0

    def update(self):
        logging.info('update 资产负债表(balancesheet) ...')

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

            logging.debug('get {}-{} balancesheet of {} from tushare'.format(
                start_date, end_date, ts_code))

            data = None
            delay_time = 61
            while True:
                try:
                    data = self.getBalancesheet(ts_code, start_date, end_date, report_type='1')
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
            'update balancesheet finished: {} checked, {} inserted, {} replaced'
            .format(checked_total, inserted_total, replaced_total))
