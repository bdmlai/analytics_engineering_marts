/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_cpg_venue_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture merch sales for talents at venue level

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent_equity','cpg','venue','daily','merch',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

SELECT cast('venue' as varchar(10)) as dim_business_unit_id,v.date_id as date, i.src_talent_description,
trim(i.src_style_description,' ') as src_style_description,trim(i.src_category_description,' ') as src_category_description 
,SUM(v.net_units_sold) as demand_units, SUM(v.total_revenue) as demand_sales
FROM {{source('sf_fds_cpg','aggr_cpg_daily_venue_sales')}} v
LEFT JOIN {{source('sf_fds_cpg','dim_cpg_item')}} i ON v.dim_item_id = i.dim_item_id
GROUP BY 1,2,3,4,5

)
select * from 
    source_data 