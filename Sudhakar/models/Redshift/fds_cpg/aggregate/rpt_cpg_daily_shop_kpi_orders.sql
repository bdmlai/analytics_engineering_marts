{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'table','tags': "Phase 5B","persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": 'grant select on {{this}} to public'
		})
}}
SELECT
    z.business_unit,
    v.date AS date,
    z.dim_shop_site_id,
    SUM(
        CASE
            WHEN z.date = v.date
            THEN orders
            ELSE 0
        END) orders,
    SUM(
        CASE
            WHEN z.date = v.date
            THEN customers
            ELSE 0
        END) customers,
    SUM(
        CASE
            WHEN z.date = v.date
            THEN sales
            ELSE 0
        END) sales,
    SUM(
        CASE
            WHEN z.date = v.date
            THEN units
            ELSE 0
        END)                                            units,
    SUM(orders)                                         AS orders_ttm,
    SUM(customers)                                      AS customer_ttm,
    SUM(sales)                                          AS sales_ttm,
    SUM(units)                                          AS units_ttm,
    'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' AS etl_batch_id,
    'bi_dbt_user_prd'                                   AS etl_insert_user_id,
    SYSDATE                                             AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST(NULL AS TIMESTAMP)                             AS etl_update_rec_dttm
FROM
    (
        SELECT
            'shop' AS business_unit,
            date,
            dim_shop_site_id,
            COUNT(DISTINCT src_wwe_order_number)          AS orders,
            COUNT(DISTINCT dim_customer_email_address_id) AS customers,
            SUM(sell_amount)                              AS sales,
            SUM(src_units_ordered)                        AS units
        FROM
            (
                SELECT
                    *,
                    CAST(CAST(order_date_id AS VARCHAR(10)) AS DATE) AS date,
                    src_units_ordered * ISNULL(src_selling_price, 0) AS sell_amount
                FROM
                    (
                        SELECT
                            dim_customer_email_address_id,
                            order_date_id,
                            dim_shop_site_id,
                            src_wwe_order_number,
                            src_selling_price,
                            src_units_ordered
                        FROM
                            {{source('fds_cpg','fact_cpg_sales_detail')}}
                        WHERE
                            dim_shop_site_id NOT IN (12,
                                                     13)
                        AND (
                                order_date_id >= 0
                            OR  ship_date_id >= 0)
                        AND ISNULL(dim_order_method_id, 0) <> 82005
                        AND dim_item_id NOT IN
                                                (
                                                SELECT DISTINCT
                                                    b.dim_item_id
                                                FROM
                                                    {{source('fds_cpg','dim_cpg_kit_item')}} a
                                                JOIN
                                                    {{source('fds_cpg','dim_cpg_item')}} b
                                                ON
                                                    a.src_kit_id = b.src_item_id)
                        AND src_order_number NOT IN
                                                     (
                                                     SELECT DISTINCT
                                                         trim(src_order_number) AS src_order_number
                                                     FROM
                                                         {{source('fds_cpg','fact_cpg_sales_header'
                                                         )}}
                                                     WHERE
                                                         trim(src_order_origin_code) = 'GR'
                                                     OR  trim(src_prepay_code) = 'F')
                        AND src_order_number IN
                                                 (
                                                 SELECT DISTINCT
                                                     trim(src_order_number) AS src_order_number
                                                 FROM
                                                     {{source('fds_cpg','fact_cpg_sales_header')}}
                                                 WHERE
                                                     trim(src_original_ref_order_number) = '0' ) )
            )
        GROUP BY
            1,2,3) z,
    (
        SELECT DISTINCT
            CAST(CAST(a.date_key AS VARCHAR(10)) AS DATE) date
        FROM
            {{source('fds_cpg','aggr_cpg_daily_sales')}} a
        WHERE
            CAST(CAST(a.date_key AS VARCHAR(10)) AS DATE)>= DATEADD('day', -1461, CURRENT_DATE)) v
WHERE
    z.date >= add_months(v.date, -12)
AND z.date <= v.date
GROUP BY
    1,2,3