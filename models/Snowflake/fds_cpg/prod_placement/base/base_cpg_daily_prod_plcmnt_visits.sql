/*
*************************************************************************************************************************************************
   TableName   : base_cpg_daily_prod_plcmnt_visits
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : To check on visits
   Version      Date            Author               Request
   1.0          03/15/2021      schakraborty            ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','hits_time_est'],
                    post_hook = "grant select on {{ this }} to DA_SCHAKRABORTY_USER_ROLE"
 ) }}
with source_data as
(SELECT date,
visit_Start_Time_est as hits_time_est,
concat(full_Visitor_Id,visit_Id) as visits,
value as productSku,
hit_product_v2ProductName as productName,
hit_transaction_transaction_Revenue/1000000 as rev, 
hit_item_transaction_Id as trans
FROM {{source('pp_udl_cpg','ga_cpg_daily_hits')}}, lateral split_to_table(hit_product_productSku,',') where date>= '2020-01-01'
ORDER by hits_time_est 
)
select * from source_data