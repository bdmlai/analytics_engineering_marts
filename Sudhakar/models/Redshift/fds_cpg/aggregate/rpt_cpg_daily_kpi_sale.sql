{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["truncate fds_cpg.rpt_cpg_daily_kpi_sale"],
		"materialized": 'incremental','tags': "Phase 5B"
  })
}}
select z.src_category_description, z.src_major_category_description, 
z.business_unit, z.dim_shop_site_id, v.date as date,
sum(case when z.date = v.date then sales else 0 end) sales, 
sum(case when z.date = v.date then units else 0 end) units,
sum(case when z.date = v.date then margin else 0 end) margin, 
sum(case when z.date = v.date then shipped_sales else 0 end) shipped_sales,
sum(sales) as sales_ttm, 
sum(units) as units_ttm, 
sum(margin) as margin_ttm, 
sum(shipped_sales) as shipped_sales_ttm,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from 
((select 'shop' as business_unit, cast(cast(a.date_key as varchar(10)) as date) as date, a.dim_shop_site_id, 
b.src_category_description, b.src_major_category_description, sum(demand_sales_$) as sales, sum(a.src_units_ordered) as units,
sum(demand_selling_margin_$) as margin, sum(shipped_sales_$) as shipped_sales
from {{source('fds_cpg','aggr_cpg_daily_sales')}} a 
left join {{source('fds_cpg','dim_cpg_item')}} b on a.dim_item_id = b.dim_item_id 
where cast(cast(a.date_key as varchar(10)) as date)>= dateadd('day', -1826, current_date)
group by 1,2,3,4,5
)
union all
(select 'venue' as business_unit, cast(cast(date_id as varchar(10)) as date) as date, 13 as dim_shop_site_id,
s.src_category_description, s.src_major_category_description, sum(total_revenue) as sales, sum(net_units_sold) as units, 
0 as margin, 0 as shipped_sales
from (select distinct date_id, dim_event_id, dim_venue_id, quantity_shipped, quantity_adjustment, quantity_returned,
compelements, net_units_sold, selling_price, total_revenue, complement_revenue, src_style_description, src_category_description,src_major_category_description, src_talent_description, src_style_id, src_item_id 
from {{source('fds_cpg','aggr_cpg_daily_venue_sales')}} a 
left join {{source('fds_cpg','dim_cpg_item')}} b on a.dim_item_id = b.dim_item_id   
where cast(cast(a.date_id as varchar(10)) as date)>= dateadd('day', -1826, current_date)
and a.active_flag = 'Y') s
group by 1,2,3,4,5)) z,
(select distinct cast(cast(a.date_key as varchar(10)) as date) date 
from {{source('fds_cpg','aggr_cpg_daily_sales')}} a 
where
cast(cast(a.date_key as varchar(10)) as date)>= dateadd('day', -1461, current_date)) v
where
z.date >= add_months(v.date, -12) and z.date <= v.date 
group by 1,2,3,4,5