{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'incremental','tags': "aggr_cpg_daily_venue_sales",
		"unique_key": 'dim_agg_sales_id',
		"incremental_strategy": 'delete+insert'
  })
}}




SELECT
    df.dim_agg_sales_id,
    df.date_id,
    df.dim_item_id,
    df.dim_event_id,
    df.dim_venue_id,
    df.quantity_shipped,
    df.quantity_adjustment,
    df.quantity_returned,
    df.compelements,
    df.net_units_sold,
    df.selling_price,
    df.total_revenue,
    df.complement_revenue ,
    'N' AS active_flag,
    df.effective_start_datetime,
    SYSDATE AS effective_end_datetime ,
    df.etl_batch_id,
    df.etl_insert_user_id,
    df.etl_insert_rec_dttm,
 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' AS etl_update_user_id,
    SYSDATE                                             AS etl_update_rec_dttm 
   
FROM
    (
 SELECT
                    UPPER(ltrim(RTRIM(b.src_item_id))) src_item_id,
                    a.*
                FROM
                    {{this}} a
                LEFT JOIN
                    {{source('fds_cpg','dim_cpg_item')}} b
                ON
                    a.dim_item_id=b.dim_item_id
                WHERE
                    LOWER(a.active_flag)='y'
)					df
INNER JOIN
   (

SELECT DISTINCT
    time_key                             AS date_id,
    Item_key                             AS src_item_id,
    event_key                            AS dim_event_id,
    venue_key                            AS dim_venue_id,
    CAST(quantity_shipped AS INTEGER)       quantity_shipped ,
    CAST(quantity_adjustment AS INTEGER)    quantity_adjustment,
    CAST(quantity_returned AS INTEGER)      quantity_returned ,
    CAST(compelements AS INTEGER)           compelements,
    CAST(net_units_sold AS INTEGER)         net_units_sold,
    CAST(Selling_price AS NUMERIC(28,6))    Selling_price,
    ROUND(total_revenue)                    total_revenue,
    complement_revenue
FROM
    {{ref('intm_cpg_venue_combined_sales')}}
EXCEPT
SELECT
    date_id,
    UPPER(ltrim(RTRIM(b.src_item_id))) AS src_item_id,
    dim_event_id,
    dim_venue_id,
    quantity_shipped,
    quantity_adjustment,
    quantity_returned,
    compelements,
    net_units_sold,
    CAST(Selling_price AS NUMERIC(28,6)) selling_price,
    ROUND(total_revenue)                 total_revenue,
    complement_revenue
FROM
    {{this}} a
LEFT JOIN
    {{source('fds_cpg','dim_cpg_item')}} b
ON
    a.dim_item_id=b.dim_item_id
WHERE
    LOWER(a.active_flag)='y' 
	) T
	 
	 ON
            df.src_item_id = T.src_item_id
        AND df.dim_event_id = T.dim_event_id
        AND df.dim_venue_id = T.dim_venue_id
        AND df.date_id = T.date_id
		
		
		union all
		
		SELECT
    (
        SELECT
            MAX(dim_agg_sales_id)
        FROM
            {{this}}) + row_number() OVER () AS dim_agg_sales_id,
    date_id,
    I.dim_item_id,
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
    'Y'                                      AS active_flag,
    CURRENT_TIMESTAMP                        AS effective_start_datetime,
    CAST('2050-12-31 00:00:00' AS TIMESTAMP) AS effective_end_datetime,
    50001                                    AS etl_batch_id,
    'bi_dbt_user_prd'                        AS etl_insert_user_id,
    CURRENT_TIMESTAMP                        AS etl_insert_rec_dttm,
    NULL                                     AS etl_update_user_id,
    CAST(NULL AS TIMESTAMP)                  AS etl_update_rec_dttm
	from (
	
	
SELECT DISTINCT
    time_key                             AS date_id,
    Item_key                             AS src_item_id,
    event_key                            AS dim_event_id,
    venue_key                            AS dim_venue_id,
    CAST(quantity_shipped AS INTEGER)       quantity_shipped ,
    CAST(quantity_adjustment AS INTEGER)    quantity_adjustment,
    CAST(quantity_returned AS INTEGER)      quantity_returned ,
    CAST(compelements AS INTEGER)           compelements,
    CAST(net_units_sold AS INTEGER)         net_units_sold,
    CAST(Selling_price AS NUMERIC(28,6))    Selling_price,
    ROUND(total_revenue)                    total_revenue,
    complement_revenue
FROM
    {{ref('intm_cpg_venue_combined_sales')}}
EXCEPT
SELECT
    date_id,
    UPPER(ltrim(RTRIM(b.src_item_id))) AS src_item_id,
    dim_event_id,
    dim_venue_id,
    quantity_shipped,
    quantity_adjustment,
    quantity_returned,
    compelements,
    net_units_sold,
    CAST(Selling_price AS NUMERIC(28,6)) selling_price,
    ROUND(total_revenue)                 total_revenue,
    complement_revenue
FROM
    {{this}} a
LEFT JOIN
    {{source('fds_cpg','dim_cpg_item')}} b
ON
    a.dim_item_id=b.dim_item_id
WHERE
    LOWER(a.active_flag)='y') T
	
	
	LEFT OUTER JOIN
            {{ref('intm_cpg_venue_dim_item')}} I
        ON
            UPPER(ltrim(RTRIM(I.src_item_id))) = UPPER(ltrim(RTRIM(T.src_item_id)))