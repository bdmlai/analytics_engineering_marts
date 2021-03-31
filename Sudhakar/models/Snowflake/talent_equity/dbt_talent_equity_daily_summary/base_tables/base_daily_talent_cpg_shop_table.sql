
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
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent equity','cpg','shop','merch','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(
select distinct cast('shop' as varchar(10)) as dim_business_unit_id,a.order_date_id as date,
a.src_style_description,a.src_category_description,a.src_talent_description,
sum(src_units_ordered) as demand_units,sum(src_units_ordered*nvl(src_selling_price,0)) as demand_sales
from (select distinct dim_business_unit_id,order_date_id,src_selling_price,src_units_ordered,
      src_style_description,src_category_description,src_talent_description,src_item_id
 from 
 (select distinct a.*,b.src_style_description,b.src_category_description,b.src_talent_description
 FROM 
 (select distinct a.*,c.src_item_id,b.src_total_units_on_hand,b.src_reserved_units,b.src_available_units 
 from 
 (select * from {{source('sf_fds_cpg','fact_cpg_sales_detail_1')}} where dim_shop_site_id<=6 and
 (order_date_id>=0 or ship_date_id>=0) and 
 nvl(dim_order_method_id,0) NOT IN (5) and 
 dim_item_id not in(
 select distinct B.dim_item_id from (select A.*,B.src_item_id from {{source('sf_fds_cpg','dim_cpg_kit_item')}} A inner 
 join {{source('sf_fds_cpg','dim_cpg_item')}} B on A.dim_kit_item_id=B.dim_item_id) A, {{source('sf_fds_cpg','dim_cpg_item')}} B 
 where A.src_item_id=B.src_item_id )
 and src_order_number not in(
 SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number FROM {{source('sf_fds_cpg','fact_cpg_sales_header')}} 
 where ltrim(rtrim(src_order_origin_code))='GR' or ltrim(rtrim(src_prepay_code))='F') 
 and src_order_number in(
 SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number FROM {{source('sf_fds_cpg','fact_cpg_sales_header')}} 
 where ltrim(rtrim(src_original_ref_order_number))='0') 
 )a 
 left join {{source('sf_fds_cpg','dim_cpg_item')}} c on a.dim_item_id=c.dim_item_id
 left join {{source('sf_fds_cpg','fact_cpg_inventory')}} b on a.dim_item_id=b.dim_item_id and a.order_date_id=b.dim_date_id 
 and a.dim_business_unit_id=b.dim_business_unit_id and a.dim_shop_site_id=b.dim_shop_site_id) a 
 left join (select * from (select *,row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank 
 from {{source('sf_fds_cpg','dim_cpg_item')}} ) where rank=1) b 
 on a.src_item_id=b.src_item_id) ) a 
group by 1,2,3,4,5

)
select * from 
    source_data