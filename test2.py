# coding: utf-8
import tushare as ts
ts.set_token('75f48b9c2d4499ef1e37b013ad7acd1583c63afcefababba16417e3a')
pro = ts.pro_api()
income = pro.income(ts_code='000651.SZ',
           fields=
           'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,ebit,ebitda,insurance_exp,undist_profit,distable_profit'
           )
income.iloc[[0]].to_dict(orient='records')
income.iloc[[0]].to_dict(orient='records')[0]['n_income_attr_p']
211.18/880.49
211.18/866.85
211.18/(2475.23-1594.74)
