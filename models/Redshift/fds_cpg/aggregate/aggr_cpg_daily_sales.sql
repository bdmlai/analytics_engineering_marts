{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'table','tags': "aggr_cpg_daily_sales" ,"persist_docs": {'relation' : true, 'columns' : true},
	      "post-hook" : 'grant select on {{this}} to public'

  })
}}


select dim_business_unit_id,
dim_order_method_id,
date_key,
src_order_type,
dim_item_id,
src_unit_cost,
src_current_retail_price,
src_units_ordered,
src_units_shipped,
units_returned,
net_units_sold,
demand_cogs_$,
shipped_cogs_$,
returned_cogs_$,
net_cogs_$,
demand_retail_$,
shipped_retail_$,
net_retail_$,
demand_sales_$,
shipped_sales_$,
returns_$,
net_sales_$,
case when src_unit_cost is null or src_unit_cost = 0 then 0 else demand_selling_margin_$ end as demand_selling_margin_$ ,
case when src_unit_cost is null or src_unit_cost = 0 then 0 else shipped_selling_margin_$ end as shipped_selling_margin_$,
case when src_unit_cost is null or src_unit_cost = 0 then 0 else net_selling_margin_$ end as net_selling_margin_$ ,
other_amount,
(src_unit_cost/nullif(conversion_rate_to_local,0)) as unit_cost_local,
(src_current_retail_price/nullif(conversion_rate_to_local,0)) as current_retail_price_local,
(demand_cogs_$/nullif(conversion_rate_to_local,0)) as demand_cogs_local,
(shipped_cogs_$/nullif(conversion_rate_to_local,0)) as shipped_cogs_local,
(returned_cogs_$/nullif(conversion_rate_to_local,0)) as returned_cogs_local,
(net_cogs_$/nullif(conversion_rate_to_local,0)) as net_cogs_local,
(demand_retail_$/nullif(conversion_rate_to_local,0)) as demand_retail_local,
(shipped_retail_$/nullif(conversion_rate_to_local,0)) as shipped_retail_local,
(net_retail_$/nullif(conversion_rate_to_local,0)) as net_retail_local,
(demand_sales_$/nullif(conversion_rate_to_local,0)) as demand_sales_local,
(shipped_sales_$/nullif(conversion_rate_to_local,0)) as shipped_sales_local,
(returns_$/nullif(conversion_rate_to_local,0)) as returns_local,
(net_sales_$/nullif(conversion_rate_to_local,0)) as net_sales_local,
case when unit_cost_local is null or unit_cost_local = 0 then 0 else(demand_selling_margin_$/nullif(conversion_rate_to_local,0)) end as demand_selling_margin_local,
case when unit_cost_local is null or unit_cost_local = 0 then 0 else (shipped_selling_margin_$/nullif(conversion_rate_to_local,0)) end as shipped_selling_margin_local,
case when unit_cost_local is null or unit_cost_local = 0 then 0 else (net_selling_margin_$/nullif(conversion_rate_to_local,0)) end as net_selling_margin_local,
(other_amount/nullif(conversion_rate_to_local,0)) as other_amount_local,
src_currency_code_from,
dim_shop_site_id,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_FDS_CPG' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from 
(
	select src.*,
	coalesce((case when src.src_currency_code_from='USD' then 1 else cc.currency_conversion_rate_spot_rate end),0) as conversion_rate_to_local
	from {{ref('intm_cpg_sales')}} src
	left outer join (select * from {{source('dt_stage','prestg_cpg_currency_exchange_rate')}} ) cc
	on cast(cast(src.date_key as varchar(20)) as date) =cc.as_on_date and src.src_currency_code_from=cc.currency_code_from and cc.currency_code_to='USD'
)