/*
*************************************************************************************************************************************************
   TableName   : base_cpg_daily_prod_plcmnt_items
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : To find distinct items and descriptions
   Version      Date            Author               Request
   1.0          03/15/2021      schakraborty            ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','src_item_description'],
                    post_hook = "grant select on {{ this }} to DA_SCHAKRABORTY_USER_ROLE"
 ) }}
with source_data as
(SELECT distinct 
src_item_id,
src_talent_description 
from {{source('pp_fds_cpg','dim_cpg_item')}}
)
select * from source_data