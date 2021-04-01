
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
select distinct cast('venue' as varchar(10)) as dim_business_unit_id,date_id as date,
src_style_description,src_category_description,
src_talent_description, sum(net_units_sold) as demand_units, sum(total_revenue) as demand_sales
from 
 (select distinct a.date_id,a.src_item_id,a.net_units_sold,a.total_revenue,
 b.src_style_description,b.src_category_description,b.src_talent_description
 from
 (select distinct a.date_id,a.dim_item_id,a.dim_event_id,a.dim_venue_id,a.net_units_sold,a.total_revenue,b.src_item_id
 from {{source('sf_fds_cpg','aggr_cpg_daily_venue_sales')}} a 
 left join {{source('sf_fds_cpg','dim_cpg_item')}} b on a.dim_item_id=b.dim_item_id)a
 left join (select * from (select *,row_number() over(partition by src_item_id order by effective_start_datetime desc) 
 as rank from {{source('sf_fds_cpg','dim_cpg_item')}} ) where rank=1) b 
 on a.src_item_id=b.src_item_id) 
group by 1,2,3,4,5

)
select * from 
    source_data