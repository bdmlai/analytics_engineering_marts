{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}

SELECT
    
    v.date AS date,
    z.dim_shop_site_id,
    SUM(
        CASE
            WHEN z.date = v.date
            THEN orders
            ELSE 0
        END) orders
    FROM
    (
        SELECT
                        date,
            dim_shop_site_id,
            count(distinct(case when src_wwe_order_number = 0 or src_wwe_order_number is NULL then src_order_number else src_wwe_order_number end)) as orders
            FROM
            (
                SELECT
                    *,
                    CAST(CAST(order_date_id AS VARCHAR(10)) AS DATE) AS date
                FROM
                    (
                        SELECT
                            order_date_id,
                            dim_shop_site_id,
                            src_wwe_order_number,
							src_order_number,
                            src_selling_price,
                            src_units_ordered
                        FROM
                            {{source('fds_cpg','fact_cpg_sales_detail')}}
                        WHERE
                            dim_shop_site_id NOT IN (13)
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
                                                         {{source('fds_cpg','fact_cpg_sales_header')}}
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
            1,2) z,
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
    1,2