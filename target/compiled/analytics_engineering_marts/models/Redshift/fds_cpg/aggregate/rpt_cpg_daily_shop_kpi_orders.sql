
select z.business_unit, v.date as date, z.dim_shop_site_id,
sum(case when z.date = v.date then orders else 0 end) orders,
sum(case when z.date = v.date then customers else 0 end) customers,
sum(case when z.date = v.date then sales else 0 end) sales,
sum(case when z.date = v.date then units else 0 end) units,
sum(orders) as orders_ttm,
sum(customers) as customer_ttm,
sum(sales) as sales_ttm,
sum(units) as units_ttm,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(select 'shop' as business_unit, date, dim_shop_site_id, count(distinct src_wwe_order_number) as orders,
count(distinct dim_customer_email_address_id) as customers, sum(sell_amount) as sales, sum(src_units_ordered) as units
from 
(select *, cast(cast(order_date_id as varchar(10)) as date) as date, src_units_ordered * isnull(src_selling_price, 0) as sell_amount
	from
      (select dim_customer_email_address_id, order_date_id, dim_shop_site_id, 
				src_wwe_order_number, src_selling_price, src_units_ordered
		from "entdwdb"."fds_cpg"."fact_cpg_sales_detail" 
		where  dim_shop_site_id <= 11 and (order_date_id >= 0 or ship_date_id >= 0) and
				isnull(dim_order_method_id, 0) <> 82005 and
				dim_item_id not in (select distinct b.dim_item_id from "entdwdb"."fds_cpg"."dim_cpg_kit_item" a 
									join "entdwdb"."fds_cpg"."dim_cpg_item" b on a.src_kit_id = b.src_item_id) 
				and src_order_number not in (select distinct trim(src_order_number) as src_order_number
                      from "entdwdb"."fds_cpg"."fact_cpg_sales_header" 
					  where trim(src_order_origin_code) = 'GR' or trim(src_prepay_code) = 'F')
				and src_order_number  in (select distinct trim(src_order_number) as src_order_number
                      from "entdwdb"."fds_cpg"."fact_cpg_sales_header" 
					  where trim(src_original_ref_order_number) = '0'
                      ) ) ) group by 1,2,3) z,
(select distinct cast(cast(a.date_key as varchar(10)) as date) date 
from "entdwdb"."fds_cpg"."aggr_cpg_daily_sales" a 
where
cast(cast(a.date_key as varchar(10)) as date)>= dateadd('day', -1461, current_date)) v
where
z.date >= add_months(v.date, -12) and z.date <= v.date
group by 1,2,3