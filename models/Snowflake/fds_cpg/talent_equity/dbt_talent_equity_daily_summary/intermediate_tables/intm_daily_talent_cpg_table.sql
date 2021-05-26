
/*
*************************************************************************************************************************************************
   TableName   : intm_daily_talent_cpg_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Intermediate Model to capture merch sales for talents 

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','cpg','shop','venue','daily','merch'
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

select b.full_date as date,a.dim_business_unit_id,src_style_description,src_category_description, 
src_talent_description,sum(demand_units) as demand_units,sum(demand_sales) as demand_sales
from 
(
    select * from {{ref('base_daily_talent_cpg_shop_table')}}
    union all
    select * from {{ref('base_daily_talent_cpg_venue_table')}}
) a
left join {{source('sf_cdm','dim_date')}} b on a.date=b.dim_date_id 
group by 1,2,3,4,5
)
select * from 
    source_data