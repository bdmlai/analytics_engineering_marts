{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["truncate fds_cpg.rpt_cpg_monthly_talent_overall_shop_sales"],
		"materialized": 'incremental','tags': "Phase 5B"
  })
}}
select a.*, py_styles_selling, py_revenue, py_margin, 
py_units_ordered, py_avg_unit_price, py_cust_count, py_new_styles,
pm_rev_rank, pm_units_rank, pm_style_sold_rank, pm_avg_unit_p_rank,
AUP_percentile, pm_sale_data.rev as pm_revenue, pm_margin, pm_sale_data.units as pm_units_ordered,
pm_sale_data.avg_unit_p as pm_avg_unit_price, pm_cust_count, pm_styles_selling, pm_overall_revenue,
pm_overall_margin, pm_overall_units_ordered, pm_overall_avg_unit_price, pm_overall_styles_selling,
overall_revenue, overall_margin, overall_units_ordered, overall_avg_unit_price, overall_styles_selling,
rev_rank, units_rank, style_sold_rank, avg_unit_p_rank, pm_new_styles,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from {{ref('intm_cpg_monthly_talent_shop_sales')}} a
left join 
(select dateadd('year', 1, order_month)  as order_month, talent_description ,
count(distinct style_category) as py_styles_selling, sum(revenue) as py_revenue, 
sum(margin) as py_margin, sum(units_ordered) as py_units_ordered,
(sum(revenue)/nullif(sum(units_ordered),0)) as py_avg_unit_price, sum(customers_cnt) as py_cust_count
from {{ref('intm_cpg_monthly_talent_shop_sales')}}
group by 1,2) py_sale_data
on a.talent_description = py_sale_data.talent_description
and a.order_month = py_sale_data.order_month
left join
(select dateadd('year', 1, order_month) as order_month, talent_description, count(distinct style_category) as py_new_styles
from {{ref('intm_cpg_monthly_talent_shop_sales')}} where style_launched_90_days = 'Yes'
group by 1,2) py_new_style_data
on a.talent_description = py_new_style_data.talent_description
and a.order_month = py_new_style_data.order_month
left join 
(select date_add('month', 1, order_month) as order_month, talent_description, count(distinct style_category) as pm_new_styles
from  {{ref('intm_cpg_monthly_talent_shop_sales')}} where style_launched_90_days = 'Yes'
group by 1,2) pm_new_style_data
on a.order_month = pm_new_style_data.order_month
and a.talent_description = pm_new_style_data.talent_description
left join
(select order_month, talent_description, rev, units, style_sold, avg_unit_p,
	row_number() over (partition by order_month order by rev desc) as rev_rank,
	row_number() over (partition by order_month order by units desc) as units_rank,
	row_number() over (partition by order_month order by style_sold desc) as style_sold_rank,
	row_number() over (partition by order_month order by avg_unit_p desc) as avg_unit_p_rank,
	round((percent_rank() over (partition by order_month order by avg_unit_p asc))*100,2) as AUP_percentile
from
(select date_trunc('month', cast(cast(date_key as varchar(10)) as date)) order_month, talent_description,
sum(isnull(demand_sales_$,0))  as rev, sum(src_units_ordered) as units, count(distinct src_style_description) as style_sold,
(sum(isnull(demand_sales_$,0))/nullif(sum(src_units_ordered),0)) as avg_unit_p from   
{{source('fds_cpg','aggr_cpg_daily_sales')}} m inner join
(select v.dim_item_id,
case 
when v.src_talent_description like '%ALMAS%' then 'ANDRADE ''CIEN'' ALMAS'
when v.src_talent_description ='RANDOM' then 'RONDA ROUSEY' 
when v.src_talent_description in ('HARDY''S','HARDY BOYZ') then 'HARDY BOYZ'
when v.src_talent_description like '%ELIAS%' then 'ELIAS' 
when v.src_talent_description like '%BIG E%' then 'BIG E'
when v.src_talent_description like '%APOLLO%' then 'APOLLO CREWS'
else v.src_talent_description 
end talent_description, v.src_style_description 
from {{source('fds_cpg','dim_cpg_item')}} v
where v.src_talent_description not in ('WWE','RAW','WRESTLEMANIA','SMACKDOWN','NXT','WOMEN''S DIVISION','DIVA',
'EVOLUTION WOMEN''S PPV','TAPOUT','WWE NETWORK','ECW','BIRDIEBEE','LEGENDS','205 Live','PPV','BLANK -SALES RPT CODE 1 41/S1')) n 
on m.dim_item_id = n.dim_item_id
left join {{source('fds_cpg','dim_cpg_order_method')}} j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and date_key > 20151231  group by 1,2)) cm_rank_data
on a.talent_description = cm_rank_data.talent_description
and a.order_month = cm_rank_data.order_month
left join 
(select date_trunc('month', cast(cast(date_key as varchar(10)) as date)) order_month,
sum(isnull(demand_sales_$,0)) as pm_overall_revenue, sum(isnull(demand_selling_margin_$,0)) as pm_overall_margin,
sum(src_units_ordered)as pm_overall_units_ordered, (sum(isnull(demand_sales_$,0))/nullif(sum(src_units_ordered),0)) as pm_overall_avg_unit_price, count(distinct src_style_description) as pm_overall_styles_selling
from {{source('fds_cpg','aggr_cpg_daily_sales')}} m 
left join {{source('fds_cpg','dim_cpg_item')}} n on m.dim_item_id = n.dim_item_id
left join {{source('fds_cpg','dim_cpg_order_method')}} j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and  m.date_key > 20151231
group by 1) pm_overall_data
on a.order_month = date_add('month', 1, pm_overall_data.order_month)
left join 
(select date_trunc('month', cast(cast(date_key as varchar(10)) as date)) order_month,
sum(isnull(demand_sales_$,0)) as overall_revenue, sum(isnull(demand_selling_margin_$,0)) as overall_margin,
sum(src_units_ordered)as overall_units_ordered, (sum(isnull(demand_sales_$,0))/nullif(sum(src_units_ordered),0)) as overall_avg_unit_price,
count(distinct src_style_description) as overall_styles_selling
from {{source('fds_cpg','aggr_cpg_daily_sales')}} m 
left join {{source('fds_cpg','dim_cpg_item')}} n on m.dim_item_id = n.dim_item_id
left join {{source('fds_cpg','dim_cpg_order_method')}} j on m.dim_order_method_id = j.dim_order_method_id
where isnull(j.src_channel_id, '0') <> 'R' and  m.date_key > 20151231
 group by 1) cm_overall_data
on a.order_month = cm_overall_data.order_month
left join
(select date_add('month', 1, order_month) order_month, talent_description, 
rev, units, style_sold, avg_unit_p, pm_margin, pm_cust_count,pm_styles_selling,
row_number() over (partition by order_month order by rev desc) as pm_rev_rank,
row_number() over (partition by order_month order by units desc) as pm_units_rank,
row_number() over (partition by order_month order by style_sold desc) as pm_style_sold_rank,
row_number() over (partition by order_month order by avg_unit_p desc) as pm_avg_unit_p_rank
from
(select order_month, talent_description, sum(revenue) as rev, sum(units_ordered) as units, count(distinct style_category) as style_sold,
(sum(revenue)/nullif(sum(units_ordered),0)) as avg_unit_p, sum(margin) as pm_margin, sum(customers_cnt) as pm_cust_count,
count(distinct style_category) as pm_styles_selling
from {{ref('intm_cpg_monthly_talent_shop_sales')}} 
group by 1,2)) pm_sale_data
on a.order_month = pm_sale_data.order_month
and a.talent_description = pm_sale_data.talent_description