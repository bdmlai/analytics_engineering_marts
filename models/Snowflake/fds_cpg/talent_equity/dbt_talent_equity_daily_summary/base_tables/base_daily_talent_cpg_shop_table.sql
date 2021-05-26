/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_cpg_shop_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture merch sales for talents at shop level

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','cpg','shop','merch','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(
SELECT cast('shop' as varchar(10)) as dim_business_unit_id,a.date_key as date, i.src_talent_description, 
trim(i.src_style_description,' ') as src_style_description,trim(i.src_category_description,' ') as src_category_description 
,SUM(a.src_units_ordered) as demand_units, SUM(a.demand_sales_$) as demand_sales
FROM {{source('sf_fds_cpg','aggr_cpg_daily_sales')}} a
LEFT JOIN {{source('sf_fds_cpg','dim_cpg_item')}} i ON a.dim_item_id = i.dim_item_id
GROUP BY 1,2,3,4,5
)
select * from 
    source_data