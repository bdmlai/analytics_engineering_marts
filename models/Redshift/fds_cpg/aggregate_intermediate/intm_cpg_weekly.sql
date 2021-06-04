 
{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}



select b.*, 
a.orders as orders_wk,
a.revenue as revenue_wk,
a.avg_order_size as avg_order_size_wk,
'CPG'::varchar(12) as platform
from 
{{ref("intm_cpg_dim_dates")}} b
left join 
(select 
date_trunc('week',a.date) as monday_date,
sum(b.orders) as orders,
sum(a.sales) as revenue,
sum(a.sales)/sum(b.orders) as avg_order_size
from
{{ref("intm_cpg_revenue")}} as a
left join
{{ref("intm_cpg_orders")}} as b
on a.date=b.date and a.dim_shop_site_id=b.dim_shop_site_id
group by 1) a
on a.monday_date = b.cal_year_mon_week_begin_date



