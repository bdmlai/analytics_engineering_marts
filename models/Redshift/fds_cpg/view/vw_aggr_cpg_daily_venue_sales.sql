{{
  config({
		 'schema': 'fds_cpg',	
	     "materialized": 'view','tags': "aggr_cpg_daily_venue_sales","persist_docs": {'relation' : true, 'columns' : true}
        })
}}
select  dim_agg_sales_id,
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
    active_flag,
    effective_start_datetime,
    effective_end_datetime,
    etl_batch_id,
    etl_insert_user_id,
    etl_insert_rec_dttm,
    etl_update_user_id,
    etl_update_rec_dttm
 from {{ref('aggr_cpg_daily_venue_sales')}}