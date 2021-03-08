
SELECT
    (select max(dim_agg_sales_id) from "entdwdb"."fds_cpg"."aggr_cpg_daily_venue_sales") + row_number() OVER () as dim_agg_sales_id,
    date_id,
    dim_item_id,
    dim_event_id,
    dim_venue_id,
    quantity_shipped,
    quantity_adjustment,
    quantity_returned,
    compelements,
    net_units_sold,
    selling_price,
    total_revenue,
    complement_revenue,
    'Y' as active_flag,
	current_timestamp as effective_start_datetime,
    cast('2050-12-31 00:00:00' as timestamp) as effective_end_datetime,
	50001 as etl_batch_id, 
	'bi_dbt_user_prd' as etl_insert_user_id, 
	current_timestamp as etl_insert_rec_dttm, 
	null as etl_update_user_id, 
	cast(null as timestamp) as etl_update_rec_dttm
FROM
	(select distinct date_id,dim_item_id,dim_event_id,dim_venue_id,
	quantity_shipped,quantity_adjustment,quantity_returned,compelements,
	net_units_sold,selling_price,total_revenue,complement_revenue,
	selling_ratio,ins_upd_flag 
	from #stg_fact_cpg_aggregated_venue_sales
	WHERE lower(ins_upd_flag) in  ('i','u'))