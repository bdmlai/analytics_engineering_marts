{{
  config({
		"materialized": 'ephemeral'
  })
}}
select * from 
((select distinct dim_business_unit_id as business_unit,date,dim_shop_site_id_new as site_key_new,site_description,src_item_id as item_id,
src_style_description as style_description,src_category_description as category_description,src_major_category_description as major_category_description,src_talent_description as talent_description,
sum(demand_units) as demand_units, sum(demand_sales) as demand_sales,sum(demand_margin) as demand_margin,
sum(shipped_units) as shipped_units, sum(shipped_sales) as shipped_sales,sum(shipped_margin) as shipped_margin,
sum(src_total_units_on_hand) as total_units_on_hand,sum(src_reserved_units) as reserved_units,
sum(src_available_units) as available_units
from 
(select distinct cast('Shop' as varchar(10)) as dim_business_unit_id,cast(cast(a.order_date_id as varchar(10)) as date) as date,a.dim_shop_site_id_new,
case when dim_shop_site_id_new=1 then 'US shop' when dim_shop_site_id_new=2 then 'UK Shop' when dim_shop_site_id_new=3 then 'EU Shop' when dim_shop_site_id_new=4 then 'US Amazon' when dim_shop_site_id_new=5 then 'UK Amazon' when dim_shop_site_id_new=6 then 'EU Amazon'
when dim_shop_site_id_new=7 then 'EBAY' when dim_shop_site_id_new=8 then 'WALMART' when dim_shop_site_id_new=9 then 'AMAZON' when dim_shop_site_id_new=10 then 'TAPOUT' end as site_description,
a.src_item_id,a.src_style_description,a.src_category_description,a.src_major_category_description,a.src_talent_description,
sum(src_units_ordered) as demand_units,sum(src_units_ordered*isnull(src_selling_price,0)) as demand_sales,sum(src_units_ordered*(src_selling_price-src_unit_cost)) as demand_margin,
sum(src_units_shipped) as shipped_units,sum(src_units_shipped*isnull(src_selling_price,0)) as shipped_sales,sum(src_units_shipped*(src_selling_price-src_unit_cost)) as shipped_margin,
sum(src_total_units_on_hand) as src_total_units_on_hand,sum(src_reserved_units) as src_reserved_units,
sum(src_available_units) as src_available_units
from  ( select distinct src_adj_txn_flag,src_back_order_flag,dim_business_unit_id,dim_customer_email_address_id,dim_order_method_id,src_current_retail_price,src_current_retail_price_local,
 order_date_id,src_order_number,src_order_origin_code,src_order_status,src_order_type,src_original_ref_order_number,src_pre_order_flag,src_prepay_code,src_promo_code,src_return_reason_code,
 src_sequence_number,src_ship_carrier,ship_date_id,src_shipped_flag,dim_shop_site_id,dim_source_sys_id,src_special_instructions,src_unit_cost,src_unit_cost_local,src_wwe_order_number,
 src_selling_price,src_units_ordered,src_units_shipped,dim_shop_site_id_new,src_style_description,src_category_description,src_major_category_description,src_talent_description,src_style_id,
 src_item_id,src_total_units_on_hand,src_reserved_units,src_available_units
 from  (select distinct a.*,b.src_style_id,b.src_style_description,b.src_category_description,b.src_major_category_description,b.src_talent_description,
 case when src_promo_code='EBAY' then 7 when src_promo_code='WALMART' then 8 when src_promo_code='AMAZON' then 9 
 when src_wwe_order_number like '900%' then 10 else dim_shop_site_id end as dim_shop_site_id_new
 FROM  (select distinct a.*,c.src_item_id,b.src_total_units_on_hand,b.src_reserved_units,b.src_available_units 
 from  (select * from {{source('fds_cpg','fact_cpg_sales_detail')}} where dim_shop_site_id<=6 and
 (order_date_id>=0 or ship_date_id>=0) and 
 isnull(dim_order_method_id,0) NOT IN (5) and 
 dim_item_id not in (
 select distinct B.dim_item_id from (select A.*,B.src_item_id from {{source('fds_cpg','dim_cpg_kit_item')}} (nolock) A inner join fds_cpg.dim_cpg_item(nolock) B on A.dim_kit_item_id=B.dim_item_id) A, fds_cpg.dim_cpg_item B where A.src_item_id=B.src_item_id )
 and src_order_number not in (
 SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number FROM {{source('fds_cpg','fact_cpg_sales_header')}} where ltrim(rtrim(src_order_origin_code))='GR' or ltrim(rtrim(src_prepay_code))='F') 
 and src_order_number in (
 SELECT distinct ltrim(rtrim(src_order_number)) As src_order_number FROM {{source('fds_cpg','fact_cpg_sales_header')}} (nolock) where ltrim(rtrim(src_original_ref_order_number))='0') 
 ) a 
 left join {{source('fds_cpg','dim_cpg_item')}} c on a.dim_item_id=c.dim_item_id
 left join {{source('fds_cpg','fact_cpg_inventory')}} b on a.dim_item_id=b.dim_item_id and a.order_date_id=b.dim_date_id and a.dim_business_unit_id=b.dim_business_unit_id and a.dim_shop_site_id=b.dim_shop_site_id) a 
 left join (select * from (select *,row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank from {{source('fds_cpg','dim_cpg_item')}} ) where rank=1) b 
 on a.src_item_id=b.src_item_id) 
 ) a  
group by 1,2,3,4,5,6,7,8,9 
union
select distinct 'Shop' as dim_business_unit_id,cast(cast(a.order_date_id as varchar(10)) as date) as date,a.dim_shop_site_id_new,
case when dim_shop_site_id_new=1 then 'US shop' when dim_shop_site_id_new=2 then 'UK Shop' when dim_shop_site_id_new=3 then 'EU Shop' when dim_shop_site_id_new=4 then 'US Amazon' when dim_shop_site_id_new=5 then 'UK Amazon' when dim_shop_site_id_new=6 then 'EU Amazon'
when dim_shop_site_id_new=7 then 'EBAY' when dim_shop_site_id_new=8 then 'WALMART' when dim_shop_site_id_new=9 then 'AMAZON' when dim_shop_site_id_new=10 then 'TAPOUT' end as site_description,
a.src_item_id,a.src_style_description,a.src_category_description,a.src_major_category_description,a.src_talent_description,
sum(src_kit_units_ordered) as demand_units,sum(src_kit_units_ordered*isnull((src_component_percent/100)*src_kit_selling_price,0)) as demand_sales,sum(src_kit_units_ordered*(((src_component_percent/100)*src_kit_selling_price)-a.src_unit_cost)) as demand_margin,
sum(src_kit_units_shipped) as shipped_units,sum(src_kit_units_shipped*isnull((src_component_percent/100)*src_kit_selling_price,0)) as shipped_sales,sum(src_kit_units_shipped*(((src_component_percent/100)*src_kit_selling_price)-a.src_unit_cost)) as shipped_margin,
sum(src_total_units_on_hand) as src_total_units_on_hand,sum(src_reserved_units) as src_reserved_units,
sum(src_available_units) as src_available_units
from 
 (select distinct src_adj_txn_flag,src_back_order_flag,dim_business_unit_id,dim_customer_email_address_id,dim_order_method_id,src_current_retail_price,src_current_retail_price_local,
 order_date_id,src_order_number,src_order_origin_code,src_order_status,src_order_type,src_original_ref_order_number,src_pre_order_flag,src_prepay_code,src_promo_code,src_return_reason_code,
 src_sequence_number,src_ship_carrier,ship_date_id,src_shipped_flag,dim_shop_site_id,dim_source_sys_id,src_special_instructions,src_unit_cost,src_unit_cost_local,src_wwe_order_number,src_component_percent,
 src_kit_selling_price,src_kit_units_ordered,src_kit_units_shipped,dim_shop_site_id_new,src_style_description,src_category_description,src_major_category_description,src_talent_description,src_style_id,
 src_item_id,0 as src_total_units_on_hand,0 as src_reserved_units,0 as src_available_units
 from 
 (select distinct a.*,b.src_style_id,b.src_style_description,b.src_category_description,b.src_major_category_description,b.src_talent_description,
 case when src_promo_code='EBAY' then 7 when src_promo_code='WALMART' then 8 when src_promo_code='AMAZON' then 9 
 when src_wwe_order_number like '900%' then 10 else dim_shop_site_id end as dim_shop_site_id_new
 FROM 
 (select distinct a.*,c.src_item_id from (select * from {{source('fds_cpg','fact_cpg_sales_detail_kit_component')}} ) a left join {{source('fds_cpg','dim_cpg_item')}} c on a.dim_item_id=c.dim_item_id) a 
 left join (select * from (select *,row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank from {{source('fds_cpg','dim_cpg_item')}} ) where rank=1) b 
 on a.src_item_id=b.src_item_id
 )
 ) a 
group by 1,2,3,4,5,6,7,8,9 ) 
group by 1,2,3,4,5,6,7,8,9 )

UNION
(select distinct cast('Venue' as varchar(10)) as dim_business_unit_id,cast(cast(date_id as varchar(10)) as date) as date,0 as dim_shop_site_id_new,'Venue' as site_description,
src_item_id,src_style_description,src_category_description,
src_major_category_description,src_talent_description, sum(net_units_sold) as demand_units, sum(total_revenue) as demand_sales, 0 as demand_margin,
0 as shipped_units,0 as shipped_sales,0 as shipped_margin,0 as src_total_units_on_hand,0 as src_reserved_units,0 as src_available_units 
from 
 (select distinct a.date_id,a.dim_event_id,a.src_item_id,a.dim_venue_id,a.quantity_shipped,a.quantity_adjustment,a.quantity_returned,a.compelements,a.net_units_sold,a.selling_price,a.total_revenue,a.complement_revenue,
 b.src_style_description,b.src_category_description,b.src_major_category_description,b.src_talent_description,b.src_style_id
 from
 (select distinct a.dim_agg_sales_id,a.date_id,a.dim_item_id,a.dim_event_id,a.dim_venue_id,a.quantity_shipped,a.quantity_adjustment,a.quantity_returned,
 a.compelements,a.net_units_sold,a.selling_price,a.total_revenue,a.complement_revenue,b.src_item_id
 from {{source('fds_cpg','aggr_cpg_daily_venue_sales')}} a left join {{source('fds_cpg','dim_cpg_item')}} b on a.dim_item_id=b.dim_item_id) a
 left join (select * from (select *,row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank from {{source('fds_cpg','dim_cpg_item')}} ) where rank=1) b 
 on a.src_item_id=b.src_item_id ) 
group by 1,2,3,4,5,6,7,8,9 )
---
) 

{% if is_incremental() %}
where date_trunc('month',date)=date_trunc('month',current_date-28)
{% endif %}