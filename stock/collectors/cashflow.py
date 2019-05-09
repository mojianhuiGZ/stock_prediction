# coding: utf-8
'''现金流量表
https://tushare.pro/document/2?doc_id=44
'''

import time
import logging
from datetime import datetime
from .base_collector import TushareMongodbBaseCollector
from .stock_basic import StockBasicCollector
from ..utils import singleton, getQuarterOfYears, getPeriodOfYears


@singleton
class CashflowCollector(TushareMongodbBaseCollector):

    def __init__(self,
                 token='',
                 server_ip='',
                 server_port=0,
                 username='',
                 password='',
                 database_name=''):
        super().__init__(
            collection_name='cashflow',
            primary_key={'ts_code': 1, 'report_type': 1, 'end_date': 1},
            token=token,
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            database_name=database_name)

    def getCashflow(self, ts_code, start_date=None, end_date=None,
                    report_type=None):
        tushare = self.getTushare()
        data = tushare.cashflow(
            ts_code=ts_code,
            ann_date='',
            start_date=start_date,
            end_date=end_date,
            period='',
            report_type=report_type,
            comp_type='',
            fields=
            'ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,net_profit,finan_exp,c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,n_recp_disp_fiolta,n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,im_n_incr_cash_equ'
        )
        return data.to_dict(orient='records')

    # 不替换旧记录
    def compareRecord(self, new_record, old_record):
        return 0

    def update(self):
        logging.info('update 现金流量表(cashflow) ...')

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

            logging.debug('get {}-{} cashflow of {} from tushare'.format(
                start_date, end_date, ts_code))

            data = None
            delay_time = 61
            while True:
                try:
                    data = self.getCashflow(ts_code, start_date, end_date, report_type='1')
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
            'update cashflow finished: {} checked, {} inserted, {} replaced'
            .format(checked_total, inserted_total, replaced_total))