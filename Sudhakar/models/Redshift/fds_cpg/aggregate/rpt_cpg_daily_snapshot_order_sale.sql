{{
  config({
		'schema': 'fds_cpg',
		"pre-hook": ["truncate fds_cpg.rpt_cpg_daily_snapshot_order_sale"],
		"materialized": 'incremental','tags': "Phase 5B"
  })
}}
select *, 
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
sysdate as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from 
((select distinct business_unit, date, dim_shop_site_id, site_description, src_item_id, 
src_style_description, src_category_description, src_major_category_description, src_talent_description,
sum(demand_units) as demand_units, sum(demand_sales) as demand_sales, sum(demand_margin) as demand_margin,
sum(shipped_units) as shipped_units, sum(shipped_sales) as shipped_sales, sum(shipped_margin) as shipped_margin,
sum(src_total_units_on_hand) as total_units_on_hand, sum(src_reserved_units) as reserved_units,
sum(src_available_units) as available_units
from 
(select distinct  cast('shop' as varchar(10)) as business_unit, cast(cast(a.order_date_id as varchar(10)) as date) as date, a.dim_shop_site_id,
site_abbr as site_description, a.src_item_id, a.src_style_description, a.src_category_description, a.src_major_category_description,
a.src_talent_description, sum(src_units_ordered) as demand_units, sum(src_units_ordered * isnull(src_selling_price, 0)) as demand_sales,
sum(src_units_ordered * (src_selling_price - src_unit_cost)) as demand_margin, sum(src_units_shipped) as shipped_units,
sum(src_units_shipped * isnull(src_selling_price, 0)) as shipped_sales, 
sum(src_units_shipped * (src_selling_price - src_unit_cost)) as shipped_margin,
sum(src_total_units_on_hand) as src_total_units_on_hand, sum(src_reserved_units) as src_reserved_units,
sum(src_available_units) as src_available_units
from 
        (select distinct src_adj_txn_flag, src_back_order_flag, dim_business_unit_id, dim_order_method_id, src_current_retail_price,src_current_retail_price_local, dim_customer_email_address_id, dim_region_id, dim_country_id, dim_state_prvn_id, dim_city_id,
        order_date_id, src_order_number, src_order_origin_code, src_order_status, src_order_type, src_original_ref_order_number,
		src_pre_order_flag, src_prepay_code, src_promo_code, src_return_reason_code, src_sequence_number, src_ship_carrier, ship_date_id, src_shipped_flag, dim_shop_site_id, dim_source_sys_id, src_special_instructions, src_unit_cost, src_unit_cost_local, 
		src_wwe_order_number, src_selling_price, src_units_ordered, src_units_shipped, site_abbr, src_style_description, src_category_description,src_major_category_description, src_talent_description, src_style_id, src_item_id, src_total_units_on_hand, src_reserved_units,src_available_units
        from 
                (select distinct a.*, b.src_style_id, b.src_style_description, b.src_category_description,
				 b.src_major_category_description, b.src_talent_description
                 from 
                        (select distinct a.*, d.site_abbr, c.src_item_id, 
						b.src_total_units_on_hand, b.src_reserved_units, b.src_available_units 
                        from 
                        (select * from {{source('fds_cpg','fact_cpg_sales_detail')}} where  dim_shop_site_id <= 11 and
                        (order_date_id >= 0 or ship_date_id >= 0) and   
                        isnull(dim_order_method_id, 0) <> 82005 and  
                        dim_item_id not in (select distinct b.dim_item_id from {{source('fds_cpg','dim_cpg_kit_item')}} a 
									join {{source('fds_cpg','dim_cpg_item')}} b on a.src_kit_id = b.src_item_id)
                        and src_order_number not in (select distinct trim(src_order_number) as src_order_number
													from {{source('fds_cpg','fact_cpg_sales_header')}} 
													where trim(src_order_origin_code) = 'GR' or trim(src_prepay_code) = 'F') 
                        and src_order_number  in (select distinct trim(src_order_number) as src_order_number
													from {{source('fds_cpg','fact_cpg_sales_header')}} 
													where trim(src_original_ref_order_number) = '0')
                        ) a
						left join {{source('fds_cpg','dim_cpg_shop_site')}} d on a.dim_shop_site_id = d.dim_shop_site_id
                        left join {{source('fds_cpg','dim_cpg_item')}} c on a.dim_item_id = c.dim_item_id
                        left join {{source('fds_cpg','fact_cpg_inventory')}} b on a.dim_item_id = b.dim_item_id and a.order_date_id = b.dim_date_id 
								  and a.dim_business_unit_id = b.dim_business_unit_id and a.dim_shop_site_id = b.dim_shop_site_id) a 
                left join (select * from (select *, row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank 
										  from {{source('fds_cpg','dim_cpg_item')}}) where rank = 1) b 
                on  a.src_item_id = b.src_item_id)) a 
group by 1,2,3,4,5,6,7,8,9
union
select distinct cast('shop' as varchar(10)) as business_unit, cast(cast(a.order_date_id as varchar(10)) as date) as date, a.dim_shop_site_id,
site_abbr as site_description, a.src_item_id, a.src_style_description, a.src_category_description, a.src_major_category_description,
a.src_talent_description, sum(src_kit_units_ordered) as demand_units,
sum(src_kit_units_ordered * isnull((src_component_percent/100) * src_kit_selling_price, 0)) as demand_sales,
sum(src_kit_units_ordered * (((src_component_percent/100) * src_kit_selling_price) - a.src_unit_cost)) as demand_margin,
sum(src_kit_units_shipped) as shipped_units,
sum(src_kit_units_shipped * isnull((src_component_percent/100) * src_kit_selling_price, 0)) as shipped_sales,
sum(src_kit_units_shipped * (((src_component_percent/100) * src_kit_selling_price) - a.src_unit_cost)) as shipped_margin,
sum(src_total_units_on_hand) as src_total_units_on_hand, sum(src_reserved_units) as src_reserved_units,
sum(src_available_units) as src_available_units
from 
        (select distinct src_adj_txn_flag, src_back_order_flag, dim_business_unit_id, dim_order_method_id, src_current_retail_price, src_current_retail_price_local, dim_customer_email_address_id, dim_region_id, dim_country_id, dim_state_prvnc_id, dim_city_id,
        order_date_id, src_order_number, src_order_origin_code, src_order_status, src_order_type, src_original_ref_order_number, 
		src_pre_order_flag, src_prepay_code, src_promo_code, src_return_reason_code, src_sequence_number, src_ship_carrier, ship_date_id, src_shipped_flag, dim_shop_site_id, dim_source_sys_id, src_special_instructions, src_unit_cost, src_unit_cost_local, 
		src_wwe_order_number, src_component_percent, src_kit_selling_price, src_kit_units_ordered, src_kit_units_shipped, site_abbr, src_style_description, src_category_description, src_major_category_description, src_talent_description, src_style_id,
        src_item_id, 0 as src_total_units_on_hand, 0 as src_reserved_units, 0 as src_available_units
        from 
                (select distinct a.*, b.src_style_id, b.src_style_description, b.src_category_description,
				 b.src_major_category_description, b.src_talent_description
                 from 
                        (select distinct a.*, b.site_abbr, c.src_item_id
                        from {{source('fds_cpg','fact_cpg_sales_detail_kit_component')}} a
						left join {{source('fds_cpg','dim_cpg_shop_site')}} b on a.dim_shop_site_id = b.dim_shop_site_id
                        left join {{source('fds_cpg','dim_cpg_item')}} c on a.dim_item_id = c.dim_item_id) a 
                left join (select * from (select *, row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank 
										  from {{source('fds_cpg','dim_cpg_item')}}) where rank = 1) b 
                on a.src_item_id = b.src_item_id)) a 
group by 1,2,3,4,5,6,7,8,9) 
group by 1,2,3,4,5,6,7,8,9)
union all
(select distinct cast('venue' as varchar(10)) as business_unit, cast(cast(date_id as varchar(10)) as date) as date, 13 as dim_shop_site_id,
'Venue Sales' as site_description, src_item_id, src_style_description, src_category_description,src_major_category_description, src_talent_description, sum(net_units_sold) as demand_units, sum(total_revenue) as demand_sales, 0 as demand_margin, 0 as shipped_units, 
0 as shipped_sales, 0 as shipped_margin, 0 as total_units_on_hand, 0 as reserved_units, 0 as available_units
from 
        (select distinct a.date_id, a.dim_event_id, a.src_item_id, a.dim_venue_id, a.quantity_shipped, a.quantity_adjustment, 
		a.quantity_returned, a.compelements, a.net_units_sold, a.selling_price, a.total_revenue, a.complement_revenue, b.src_style_description,
		b.src_category_description, b.src_major_category_description, b.src_talent_description, b.src_style_id
        from
               (select distinct a.dim_agg_sales_id, a.date_id, a.dim_item_id, a.dim_event_id, a.dim_venue_id, a.quantity_shipped,
			    a.quantity_adjustment, a.quantity_returned, a.compelements, a.net_units_sold, a.selling_price, a.total_revenue,
				a.complement_revenue, b.src_item_id
                from {{source('fds_cpg','aggr_cpg_daily_venue_sales')}} a left join {{source('fds_cpg','dim_cpg_item')}} b on a.dim_item_id = b.dim_item_id
				where a.active_flag = 'Y') a
                left join (select * from (select *, row_number() over(partition by src_item_id order by effective_start_datetime desc) as rank from {{source('fds_cpg','dim_cpg_item')}}) where rank = 1) b 
                on a.src_item_id = b.src_item_id)
group by 1,2,3,4,5,6,7,8,9)) 
where date >= dateadd('day', -1461, current_date)