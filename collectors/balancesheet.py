# coding: utf-8
'''资产负债表
https://tushare.pro/document/2?doc_id=36
'''

import logging
from .collectors import TushareMongodbBaseCollector


class BalancesheetCollector(TushareMongodbBaseCollector):
    collection_name = 'balancesheet'

    def __init__(self, token, server_ip, server_port, username, password,
                 database_name):
        super().__init__(token, server_ip, server_port, username, password,
                         database_name)

        self.validator = {
            '$jsonSchema': {
                'bsonType':
                'object',
                'required': [
                    'ts_code', 'ann_date', 'f_ann_date', 'end_date',
                    'report_type', 'comp_type', 'total_share', 'cap_rese',
                    'undistr_porfit', 'surplus_rese', 'special_rese',
                    'money_cap', 'trad_asset', 'notes_receiv',
                    'accounts_receiv', 'oth_receiv', 'prepayment',
                    'div_receiv', 'int_receiv', 'inventories', 'amor_exp',
                    'nca_within_1y', 'sett_rsrv', 'loanto_oth_bank_fi',
                    'premium_receiv', 'reinsur_receiv', 'reinsur_res_receiv',
                    'pur_resale_fa', 'oth_cur_assets', 'total_cur_assets',
                    'fa_avail_for_sale', 'htm_invest', 'lt_eqt_invest',
                    'invest_real_estate', 'time_deposits', 'oth_assets',
                    'lt_rec', 'fix_assets', 'cip', 'const_materials',
                    'fixed_assets_disp', 'produc_bio_assets',
                    'oil_and_gas_assets', 'intan_assets', 'r_and_d',
                    'goodwill', 'lt_amor_exp', 'defer_tax_assets',
                    'decr_in_disbur', 'oth_nca', 'total_nca', 'cash_reser_cb',
                    'depos_in_oth_bfi', 'prec_metals', 'deriv_assets',
                    'rr_reins_une_prem', 'rr_reins_outstd_cla',
                    'rr_reins_lins_liab', 'rr_reins_lthins_liab',
                    'refund_depos', 'ph_pledge_loans', 'refund_cap_depos',
                    'indep_acct_assets', 'client_depos', 'client_prov',
                    'transac_seat_fee', 'invest_as_receiv', 'total_assets',
                    'lt_borr', 'st_borr', 'cb_borr', 'depos_ib_deposits',
                    'loan_oth_bank', 'trading_fl', 'notes_payable',
                    'acct_payable', 'adv_receipts', 'sold_for_repur_fa',
                    'comm_payable', 'payroll_payable', 'taxes_payable',
                    'int_payable', 'div_payable', 'oth_payable', 'acc_exp',
                    'deferred_inc', 'st_bonds_payable', 'payable_to_reinsurer',
                    'rsrv_insur_cont', 'acting_trading_sec', 'acting_uw_sec',
                    'non_cur_liab_due_1y', 'oth_cur_liab', 'total_cur_liab',
                    'bond_payable', 'lt_payable', 'specific_payables',
                    'estimated_liab', 'defer_tax_liab',
                    'defer_inc_non_cur_liab', 'oth_ncl', 'total_ncl',
                    'depos_oth_bfi', 'deriv_liab', 'depos', 'agency_bus_liab',
                    'oth_liab', 'prem_receiv_adva', 'depos_received',
                    'ph_invest', 'reser_une_prem', 'reser_outstd_claims',
                    'reser_lins_liab', 'reser_lthins_liab', 'indept_acc_liab',
                    'pledge_borr', 'indem_payable', 'policy_div_payable',
                    'total_liab', 'treasury_share', 'ordin_risk_reser',
                    'forex_differ', 'invest_loss_unconf', 'minority_int',
                    'total_hldr_eqy_exc_min_int', 'total_hldr_eqy_inc_min_int',
                    'total_liab_hldr_eqy', 'lt_payroll_payable',
                    'oth_comp_income', 'oth_eqt_tools', 'oth_eqt_tools_p_shr',
                    'lending_funds', 'acc_receivable', 'st_fin_payable',
                    'payables', 'hfs_assets', 'hfs_sales', 'update_time'
                ],
                'properties': {
                    'ts_code': {
                        'bsonType': 'string',
                        'title': 'TS股票代码'
                    },
                    'ann_date': {
                        'bsonType': 'string',
                        'title': '公告日期'
                    },
                    'f_ann_date': {
                        'bsonType': 'string',
                        'title': '实际公告日期'
                    },
                    'end_date': {
                        'bsonType': 'string',
                        'title': '报告期'
                    },
                    'report_type': {
                        'bsonType': 'string',
                        'title': '报表类型：见下方详细说明'
                    },
                    'comp_type': {
                        'bsonType': 'string',
                        'title': '公司类型：1一般工商业 2银行 3保险 4证券'
                    },
                    'total_share': {
                        'bsonType': 'double',
                        'title': '期末总股本'
                    },
                    'cap_rese': {
                        'bsonType': 'double',
                        'title': '资本公积金 (元，下同)'
                    },
                    'undistr_porfit': {
                        'bsonType': 'double',
                        'title': '未分配利润'
                    },
                    'surplus_rese': {
                        'bsonType': 'double',
                        'title': '盈余公积金'
                    },
                    'special_rese': {
                        'bsonType': 'double',
                        'title': '专项储备'
                    },
                    'money_cap': {
                        'bsonType': 'double',
                        'title': '货币资金'
                    },
                    'trad_asset': {
                        'bsonType': 'double',
                        'title': '交易性金融资产'
                    },
                    'notes_receiv': {
                        'bsonType': 'double',
                        'title': '应收票据'
                    },
                    'accounts_receiv': {
                        'bsonType': 'double',
                        'title': '应收账款'
                    },
                    'oth_receiv': {
                        'bsonType': 'double',
                        'title': '其他应收款'
                    },
                    'prepayment': {
                        'bsonType': 'double',
                        'title': '预付款项'
                    },
                    'div_receiv': {
                        'bsonType': 'double',
                        'title': '应收股利'
                    },
                    'int_receiv': {
                        'bsonType': 'double',
                        'title': '应收利息'
                    },
                    'inventories': {
                        'bsonType': 'double',
                        'title': '存货'
                    },
                    'amor_exp': {
                        'bsonType': 'double',
                        'title': '长期待摊费用'
                    },
                    'nca_within_1y': {
                        'bsonType': 'double',
                        'title': '一年内到期的非流动资产'
                    },
                    'sett_rsrv': {
                        'bsonType': 'double',
                        'title': '结算备付金'
                    },
                    'loanto_oth_bank_fi': {
                        'bsonType': 'double',
                        'title': '拆出资金'
                    },
                    'premium_receiv': {
                        'bsonType': 'double',
                        'title': '应收保费'
                    },
                    'reinsur_receiv': {
                        'bsonType': 'double',
                        'title': '应收分保账款'
                    },
                    'reinsur_res_receiv': {
                        'bsonType': 'double',
                        'title': '应收分保合同准备金'
                    },
                    'pur_resale_fa': {
                        'bsonType': 'double',
                        'title': '买入返售金融资产'
                    },
                    'oth_cur_assets': {
                        'bsonType': 'double',
                        'title': '其他流动资产'
                    },
                    'total_cur_assets': {
                        'bsonType': 'double',
                        'title': '流动资产合计'
                    },
                    'fa_avail_for_sale': {
                        'bsonType': 'double',
                        'title': '可供出售金融资产'
                    },
                    'htm_invest': {
                        'bsonType': 'double',
                        'title': '持有至到期投资'
                    },
                    'lt_eqt_invest': {
                        'bsonType': 'double',
                        'title': '长期股权投资'
                    },
                    'invest_real_estate': {
                        'bsonType': 'double',
                        'title': '投资性房地产'
                    },
                    'time_deposits': {
                        'bsonType': 'double',
                        'title': '定期存款'
                    },
                    'oth_assets': {
                        'bsonType': 'double',
                        'title': '其他资产'
                    },
                    'lt_rec': {
                        'bsonType': 'double',
                        'title': '长期应收款'
                    },
                    'fix_assets': {
                        'bsonType': 'double',
                        'title': '固定资产'
                    },
                    'cip': {
                        'bsonType': 'double',
                        'title': '在建工程'
                    },
                    'const_materials': {
                        'bsonType': 'double',
                        'title': '工程物资'
                    },
                    'fixed_assets_disp': {
                        'bsonType': 'double',
                        'title': '固定资产清理'
                    },
                    'produc_bio_assets': {
                        'bsonType': 'double',
                        'title': '生产性生物资产'
                    },
                    'oil_and_gas_assets': {
                        'bsonType': 'double',
                        'title': '油气资产'
                    },
                    'intan_assets': {
                        'bsonType': 'double',
                        'title': '无形资产'
                    },
                    'r_and_d': {
                        'bsonType': 'double',
                        'title': '研发支出'
                    },
                    'goodwill': {
                        'bsonType': 'double',
                        'title': '商誉'
                    },
                    'lt_amor_exp': {
                        'bsonType': 'double',
                        'title': '长期待摊费用'
                    },
                    'defer_tax_assets': {
                        'bsonType': 'double',
                        'title': '递延所得税资产'
                    },
                    'decr_in_disbur': {
                        'bsonType': 'double',
                        'title': '发放贷款及垫款'
                    },
                    'oth_nca': {
                        'bsonType': 'double',
                        'title': '其他非流动资产'
                    },
                    'total_nca': {
                        'bsonType': 'double',
                        'title': '非流动资产合计'
                    },
                    'cash_reser_cb': {
                        'bsonType': 'double',
                        'title': '现金及存放中央银行款项'
                    },
                    'depos_in_oth_bfi': {
                        'bsonType': 'double',
                        'title': '存放同业和其它金融机构款项'
                    },
                    'prec_metals': {
                        'bsonType': 'double',
                        'title': '贵金属'
                    },
                    'deriv_assets': {
                        'bsonType': 'double',
                        'title': '衍生金融资产'
                    },
                    'rr_reins_une_prem': {
                        'bsonType': 'double',
                        'title': '应收分保未到期责任准备金'
                    },
                    'rr_reins_outstd_cla': {
                        'bsonType': 'double',
                        'title': '应收分保未决赔款准备金'
                    },
                    'rr_reins_lins_liab': {
                        'bsonType': 'double',
                        'title': '应收分保寿险责任准备金'
                    },
                    'rr_reins_lthins_liab': {
                        'bsonType': 'double',
                        'title': '应收分保长期健康险责任准备金'
                    },
                    'refund_depos': {
                        'bsonType': 'double',
                        'title': '存出保证金'
                    },
                    'ph_pledge_loans': {
                        'bsonType': 'double',
                        'title': '保户质押贷款'
                    },
                    'refund_cap_depos': {
                        'bsonType': 'double',
                        'title': '存出资本保证金'
                    },
                    'indep_acct_assets': {
                        'bsonType': 'double',
                        'title': '独立账户资产'
                    },
                    'client_depos': {
                        'bsonType': 'double',
                        'title': '其中：客户资金存款'
                    },
                    'client_prov': {
                        'bsonType': 'double',
                        'title': '其中：客户备付金'
                    },
                    'transac_seat_fee': {
                        'bsonType': 'double',
                        'title': '其中:交易席位费'
                    },
                    'invest_as_receiv': {
                        'bsonType': 'double',
                        'title': '应收款项类投资'
                    },
                    'total_assets': {
                        'bsonType': 'double',
                        'title': '资产总计'
                    },
                    'lt_borr': {
                        'bsonType': 'double',
                        'title': '长期借款'
                    },
                    'st_borr': {
                        'bsonType': 'double',
                        'title': '短期借款'
                    },
                    'cb_borr': {
                        'bsonType': 'double',
                        'title': '向中央银行借款'
                    },
                    'depos_ib_deposits': {
                        'bsonType': 'double',
                        'title': '吸收存款及同业存放'
                    },
                    'loan_oth_bank': {
                        'bsonType': 'double',
                        'title': '拆入资金'
                    },
                    'trading_fl': {
                        'bsonType': 'double',
                        'title': '交易性金融负债'
                    },
                    'notes_payable': {
                        'bsonType': 'double',
                        'title': '应付票据'
                    },
                    'acct_payable': {
                        'bsonType': 'double',
                        'title': '应付账款'
                    },
                    'adv_receipts': {
                        'bsonType': 'double',
                        'title': '预收款项'
                    },
                    'sold_for_repur_fa': {
                        'bsonType': 'double',
                        'title': '卖出回购金融资产款'
                    },
                    'comm_payable': {
                        'bsonType': 'double',
                        'title': '应付手续费及佣金'
                    },
                    'payroll_payable': {
                        'bsonType': 'double',
                        'title': '应付职工薪酬'
                    },
                    'taxes_payable': {
                        'bsonType': 'double',
                        'title': '应交税费'
                    },
                    'int_payable': {
                        'bsonType': 'double',
                        'title': '应付利息'
                    },
                    'div_payable': {
                        'bsonType': 'double',
                        'title': '应付股利'
                    },
                    'oth_payable': {
                        'bsonType': 'double',
                        'title': '其他应付款'
                    },
                    'acc_exp': {
                        'bsonType': 'double',
                        'title': '预提费用'
                    },
                    'deferred_inc': {
                        'bsonType': 'double',
                        'title': '递延收益'
                    },
                    'st_bonds_payable': {
                        'bsonType': 'double',
                        'title': '应付短期债券'
                    },
                    'payable_to_reinsurer': {
                        'bsonType': 'double',
                        'title': '应付分保账款'
                    },
                    'rsrv_insur_cont': {
                        'bsonType': 'double',
                        'title': '保险合同准备金'
                    },
                    'acting_trading_sec': {
                        'bsonType': 'double',
                        'title': '代理买卖证券款'
                    },
                    'acting_uw_sec': {
                        'bsonType': 'double',
                        'title': '代理承销证券款'
                    },
                    'non_cur_liab_due_1y': {
                        'bsonType': 'double',
                        'title': '一年内到期的非流动负债'
                    },
                    'oth_cur_liab': {
                        'bsonType': 'double',
                        'title': '其他流动负债'
                    },
                    'total_cur_liab': {
                        'bsonType': 'double',
                        'title': '流动负债合计'
                    },
                    'bond_payable': {
                        'bsonType': 'double',
                        'title': '应付债券'
                    },
                    'lt_payable': {
                        'bsonType': 'double',
                        'title': '长期应付款'
                    },
                    'specific_payables': {
                        'bsonType': 'double',
                        'title': '专项应付款'
                    },
                    'estimated_liab': {
                        'bsonType': 'double',
                        'title': '预计负债'
                    },
                    'defer_tax_liab': {
                        'bsonType': 'double',
                        'title': '递延所得税负债'
                    },
                    'defer_inc_non_cur_liab': {
                        'bsonType': 'double',
                        'title': '递延收益-非流动负债'
                    },
                    'oth_ncl': {
                        'bsonType': 'double',
                        'title': '其他非流动负债'
                    },
                    'total_ncl': {
                        'bsonType': 'double',
                        'title': '非流动负债合计'
                    },
                    'depos_oth_bfi': {
                        'bsonType': 'double',
                        'title': '同业和其它金融机构存放款项'
                    },
                    'deriv_liab': {
                        'bsonType': 'double',
                        'title': '衍生金融负债'
                    },
                    'depos': {
                        'bsonType': 'double',
                        'title': '吸收存款'
                    },
                    'agency_bus_liab': {
                        'bsonType': 'double',
                        'title': '代理业务负债'
                    },
                    'oth_liab': {
                        'bsonType': 'double',
                        'title': '其他负债'
                    },
                    'prem_receiv_adva': {
                        'bsonType': 'double',
                        'title': '预收保费'
                    },
                    'depos_received': {
                        'bsonType': 'double',
                        'title': '存入保证金'
                    },
                    'ph_invest': {
                        'bsonType': 'double',
                        'title': '保户储金及投资款'
                    },
                    'reser_une_prem': {
                        'bsonType': 'double',
                        'title': '未到期责任准备金'
                    },
                    'reser_outstd_claims': {
                        'bsonType': 'double',
                        'title': '未决赔款准备金'
                    },
                    'reser_lins_liab': {
                        'bsonType': 'double',
                        'title': '寿险责任准备金'
                    },
                    'reser_lthins_liab': {
                        'bsonType': 'double',
                        'title': '长期健康险责任准备金'
                    },
                    'indept_acc_liab': {
                        'bsonType': 'double',
                        'title': '独立账户负债'
                    },
                    'pledge_borr': {
                        'bsonType': 'double',
                        'title': '其中:质押借款'
                    },
                    'indem_payable': {
                        'bsonType': 'double',
                        'title': '应付赔付款'
                    },
                    'policy_div_payable': {
                        'bsonType': 'double',
                        'title': '应付保单红利'
                    },
                    'total_liab': {
                        'bsonType': 'double',
                        'title': '负债合计'
                    },
                    'treasury_share': {
                        'bsonType': 'double',
                        'title': '减:库存股'
                    },
                    'ordin_risk_reser': {
                        'bsonType': 'double',
                        'title': '一般风险准备'
                    },
                    'forex_differ': {
                        'bsonType': 'double',
                        'title': '外币报表折算差额'
                    },
                    'invest_loss_unconf': {
                        'bsonType': 'double',
                        'title': '未确认的投资损失'
                    },
                    'minority_int': {
                        'bsonType': 'double',
                        'title': '少数股东权益'
                    },
                    'total_hldr_eqy_exc_min_int': {
                        'bsonType': 'double',
                        'title': '股东权益合计(不含少数股东权益)'
                    },
                    'total_hldr_eqy_inc_min_int': {
                        'bsonType': 'double',
                        'title': '股东权益合计(含少数股东权益)'
                    },
                    'total_liab_hldr_eqy': {
                        'bsonType': 'double',
                        'title': '负债及股东权益总计'
                    },
                    'lt_payroll_payable': {
                        'bsonType': 'double',
                        'title': '长期应付职工薪酬'
                    },
                    'oth_comp_income': {
                        'bsonType': 'double',
                        'title': '其他综合收益'
                    },
                    'oth_eqt_tools': {
                        'bsonType': 'double',
                        'title': '其他权益工具'
                    },
                    'oth_eqt_tools_p_shr': {
                        'bsonType': 'double',
                        'title': '其他权益工具(优先股)'
                    },
                    'lending_funds': {
                        'bsonType': 'double',
                        'title': '融出资金'
                    },
                    'acc_receivable': {
                        'bsonType': 'double',
                        'title': '应收款项'
                    },
                    'st_fin_payable': {
                        'bsonType': 'double',
                        'title': '应付短期融资款'
                    },
                    'payables': {
                        'bsonType': 'double',
                        'title': '应付款项'
                    },
                    'hfs_assets': {
                        'bsonType': 'double',
                        'title': '持有待售的资产'
                    },
                    'hfs_sales': {
                        'bsonType': 'double',
                        'title': '持有待售的负债'
                    },
                    'update_time': {
                        'bsonType': 'date',
                        'title': '更新日期'
                    }
                }
            }
        }

    def getBalancesheet(self):
        tushare = self.getTushare()
        data = tushare.balancesheet(
            ts_code='',
            ann_date='',
            start_date='',
            end_date='',
            period='',
            report_type='',
            comp_type='',
            fields=
            'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_share,cap_rese,undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,payables,hfs_assets,hfs_sales'
        )
        return data.to_dict(orient='records')

    def update(self):
        logging.info('update 资产负债表(balancesheet) ...')

        logging.info('get balancesheet from tushare ...')
        data = self.getBalancesheet()

        logging.info('open mongodb database ...')
        logging.info('mongodb server ip and port: {}:{}'.format(
            self.getServerIP(), self.getServerPort()))
        logging.info('mongodb database and collection: {}.{}'.format(
            self.getDatabaseName(), BalancesheetCollector.collection_name))

        collection = self.openDatabase(BalancesheetCollector.collection_name,
                                       self.validator)

        logging.info('update to mongodb database ...')

        insert_count, replace_count, delete_count = self.updateDatabase(
            data, collection)

        logging.info(
            'update balancesheet finished: {} checked {} inserted {} replaced {} deleted!'
            .format(len(data), insert_count, replace_count, delete_count))
