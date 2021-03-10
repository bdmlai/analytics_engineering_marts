{{
  config({
		"materialized": 'ephemeral'
  })
}}
select dim_business_unit_id,dim_shop_site_id,src_currency_code_from,dim_order_method_id,
cast(src_order_type as varchar(1)) as src_order_type,
date_key,dim_item_id,other_amount,src_unit_cost,src_current_retail_price,
src_units_ordered,src_units_shipped,units_returned,net_units_sold,demand_cogs$ demand_cogs_$,shipped_cogs$ shipped_cogs_$,
returned_cogs$ returned_cogs_$,net_cogs$ net_cogs_$,demand_retail$ demand_retail_$,shipped_retail$ shipped_retail_$,
net_retail$ net_retail_$,demand_sales$ demand_sales_$,shipped_sales$ shipped_sales_$,return$ returns_$,
net_sales_$,demand_selling_margin$ demand_selling_margin_$,shipped_selling_margin$ shipped_selling_margin_$
,net_selling_margin$ net_selling_margin_$
 from
(
 select * from {{ref('intm_cpg_sales_regular')}} 
	union all
 select * from {{ref('intm_cpg_sales_gratis')}}
	union all
 select * from {{ref('intm_cpg_sales_free')}}
	union all
 select * from {{ref('intm_cpg_sales_others')}}
)