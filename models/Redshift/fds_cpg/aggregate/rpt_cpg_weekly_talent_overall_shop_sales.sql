{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["truncate fds_cpg.rpt_cpg_weekly_talent_overall_shop_sales"],
		"materialized": 'incremental','tags': "Phase 5B","post-hook" : 'grant select on {{this}} to public'
  })
}}
select a.gender_description_product, a.style_category, a.product_category, a.talent_description, a.active_inactive_flag,
a.gender_talent, website, style_launched_90_days, style_launched_month, avg_age_style_month, a.brand, a.order_week,
pw_rev_rank, pw_units_rank, pw_style_sold_rank, pw_avg_unit_p_rank, rev_rank, units_rank, style_sold_rank, avg_unit_p_rank,
overall_revenue, overall_margin, overall_units_ordered, overall_avg_unit_price, overall_styles_selling,
pw_overall_revenue, pw_overall_margin, pw_overall_units_ordered, pw_overall_avg_unit_price, pw_overall_styles_selling,
sum(revenue) as revenue, sum(margin) as margin, sum(units_ordered) as units_ordered, sum(revenue)/nullif(sum(units_ordered),0) as avg_unit_price,sum(customers_cnt) customers_cnt, sum(py_styles_selling) as py_styles_selling, sum(py_revenue) as py_revenue, sum(py_margin) as py_margin,
sum(py_units_ordered) as py_units_ordered, sum(py_avg_unit_price) as py_avg_unit_price, sum(py_cust_count) as py_cust_count, 
sum(py_new_styles) as py_new_styles, sum(AUP_percentile) as AUP_percentile, sum(pw_sale_data.rev) as pw_revenue,
sum(pw_sale_data.pw_margin) as pw_margin, sum(pw_sale_data.units) as pw_units_ordered, avg(pw_sale_data.avg_unit_p) as pw_avg_unit_price,
sum(pw_cust_count) as pw_cust_count, sum(pw_styles_selling) as pw_styles_selling, sum(pw_new_styles) as pw_new_styles,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from 
{{ref('intm_cpg_weekly_talent_shop_sales')}} a
left join 
(select order_week, talent_description, count(distinct style_category) as py_styles_selling,
sum(revenue) as py_revenue, sum(margin) as py_margin, sum(units_ordered) as py_units_ordered, 
(sum(revenue)/nullif(sum(units_ordered),0)) as py_avg_unit_price, sum(customers_cnt) as py_cust_count
from {{ref('intm_cpg_weekly_talent_shop_sales')}}
group by 1,2) py_sale_data
on a.talent_description = py_sale_data.talent_description
and a.order_week = date_trunc('week', dateadd('week', 52, py_sale_data.order_week))
left join
(select order_week, talent_description, count(distinct style_category) as py_new_styles
from {{ref('intm_cpg_weekly_talent_shop_sales')}} 
where style_launched_90_days = 'Yes'
group by 1,2) py_new_style_data
on a.talent_description = py_new_style_data.talent_description
and a.order_week = date_trunc('week', dateadd('week', 52, py_new_style_data.order_week))
left join
(select order_week, talent_description, rev, units, style_sold, avg_unit_p, pw_margin, pw_cust_count, pw_styles_selling,
row_number() over (partition by order_week order by rev desc) as pw_rev_rank,
row_number() over (partition by order_week order by units desc) as pw_units_rank,
row_number() over (partition by order_week order by style_sold desc) as pw_style_sold_rank,
row_number() over (partition by order_week order by avg_unit_p desc) as pw_avg_unit_p_rank
from
(select order_week, talent_description, sum(revenue) as rev, sum(units_ordered) as units, count(distinct style_category) as style_sold,
sum(margin) as pw_margin, sum(customers_cnt) as pw_cust_count, count(distinct style_category) as pw_styles_selling,
(sum(revenue)/nullif(sum(units_ordered),0)) as avg_unit_p
from {{ref('intm_cpg_weekly_talent_shop_sales')}} 
group by 1,2)) pw_sale_data
on a.order_week = dateadd('day', 7, pw_sale_data.order_week)
and a.talent_description = pw_sale_data.talent_description
left join 
(select date_trunc('week', cast(cast(date_key as varchar(10)) as date)) as order_week, 
sum(isnull(demand_sales_$,0)) as pw_overall_revenue, sum(isnull(demand_selling_margin_$,0)) as pw_overall_margin,
sum(src_units_ordered) as pw_overall_units_ordered, sum(nvl(demand_sales_$,0))/nullif(sum(src_units_ordered),0) as pw_overall_avg_unit_price,
count(distinct n.src_style_description) as pw_overall_styles_selling
from {{source('fds_cpg','aggr_cpg_daily_sales')}} m 
left join {{source('fds_cpg','dim_cpg_item')}} n on m.dim_item_id = n.dim_item_id
left join {{source('fds_cpg','dim_cpg_order_method')}} j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and m.date_key > 20151231
group by 1) pw_overall_data on a.order_week = dateadd('day', 7, pw_overall_data.order_week)
left join 
(select date_trunc('week', cast(cast(date_key as varchar(10)) as date)) order_week,
sum(isnull(demand_sales_$,0)) as overall_revenue, sum(isnull(demand_selling_margin_$,0)) as overall_margin,
sum(src_units_ordered) as overall_units_ordered, sum(nvl(demand_sales_$,0))/nullif(sum(src_units_ordered),0) as overall_avg_unit_price,
count(distinct n.src_style_description) as overall_styles_selling
from {{source('fds_cpg','aggr_cpg_daily_sales')}} m 
left join {{source('fds_cpg','dim_cpg_item')}} n on m.dim_item_id = n.dim_item_id
left join {{source('fds_cpg','dim_cpg_order_method')}} j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and m.date_key > 20151231
group by 1) cw_overall_data on a.order_week = cw_overall_data.order_week
left join 
(select order_week, talent_description, count(distinct style_category) as pw_new_styles
from {{ref('intm_cpg_weekly_talent_shop_sales')}} where style_launched_90_days = 'Yes'
group by 1, 2) pw_new_style_data
on a.order_week = dateadd('day', 7, pw_new_style_data.order_week) and 
a.talent_description = pw_new_style_data.talent_description
left join
(select order_week, talent_description,rev,units,style_sold,avg_unit_p,
row_number() over (partition by order_week order by rev desc) as rev_rank,
row_number() over (partition by order_week order by units desc) as units_rank,
row_number() over (partition by order_week order by style_sold desc) as style_sold_rank,
row_number() over (partition by order_week order by avg_unit_p desc) as avg_unit_p_rank,
round((percent_rank() over (partition by order_week order by avg_unit_p asc))*100,2) as AUP_percentile
from
(select order_week, talent_description,sum(revenue) as rev,sum(units_ordered) as units,  
count(distinct style_category) as style_sold,(sum(revenue)/nullif(sum(units_ordered),0)) as avg_unit_p 
from {{ref('intm_cpg_weekly_talent_shop_sales')}}
group by 1,2)) cw_rank_data
on a.talent_description= cw_rank_data.talent_description and 
a.order_week= cw_rank_data.order_week
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30