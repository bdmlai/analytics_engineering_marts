{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'table','tags': "Phase 5B","persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": 'grant select on {{this}} to public'		
        })
}}
SELECT DISTINCT
    a.*,
    b.revenue AS last_week_revenue,
    b.units   AS last_week_units,
    b.rank_last_week,
    c.top_product,
    'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_5B' AS etl_batch_id,
    'bi_dbt_user_prd'                                   AS etl_insert_user_id,
    SYSDATE                                             AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST(NULL AS TIMESTAMP)                             AS etl_update_rec_dttm
FROM
    (
        SELECT
            s.etl_insert_rec_dttm AS created_timestamp,
            s.dim_shop_site_id,
            CAST(CAST(s.date_key AS VARCHAR(10)) AS DATE),
            TRUNC(date_trunc('week',CAST(CAST(s.date_key AS VARCHAR(10)) AS DATE))) AS week_start,
            s.src_order_type,
            c.src_channel_name,
            ss.site_name,
            CASE
                WHEN i.src_talent_description LIKE '%ALMAS%'
                THEN 'ANDRADE ''CIEN'' ALMAS'
                WHEN i.src_talent_description = 'RANDOM'
                THEN 'RONDA ROUSEY'
                WHEN i.src_talent_description IN ('HARDY''S',
                                                  'HARDY BOYZ')
                THEN 'HARDY BOYZ'
                WHEN i.src_talent_description LIKE '%ELIAS%'
                THEN 'ELIAS'
                WHEN i.src_talent_description LIKE '%BIG E%'
                THEN 'BIG E'
                WHEN i.src_talent_description LIKE '%APOLLO%'
                THEN 'APOLLO CREWS'
                ELSE i.src_talent_description
            END AS talent_description,
            i.src_style_id,
            i.src_style_description,
            s.dim_item_id,
            SUM(s.src_units_ordered) AS units,
            SUM(s.demand_sales_$)    AS revenue
        FROM
            {{source('fds_cpg','aggr_cpg_daily_sales')}} s,
            {{source('fds_cpg','dim_cpg_item')}} i,
            {{source('fds_cpg','dim_cpg_order_method')}} c,
            {{source('fds_cpg','dim_cpg_shop_site')}} ss
        WHERE
            s.dim_item_id = i.dim_item_id
        AND s.dim_order_method_id = c.dim_order_method_id
        AND s.dim_shop_site_id = ss.dim_shop_site_id
        AND week_start = TRUNC(date_trunc('week',(CURRENT_DATE - 35)))
        GROUP BY
            s.etl_insert_rec_dttm,
            s.dim_shop_site_id,
            CAST(CAST(s.date_key AS VARCHAR(10)) AS DATE),
            s.src_order_type,
            c.src_channel_name,
            ss.site_name,
            i.src_style_id,
            i.src_talent_description,
            i.src_style_description,
            s.dim_item_id) a
LEFT JOIN
    (
        SELECT
            *,
            row_number() over (ORDER BY revenue DESC) AS rank_last_week
        FROM
            (
                SELECT
                    TRUNC(date_trunc('week',CAST(CAST(s.date_key AS VARCHAR(10)) AS DATE))) AS
                    week_start,
                    CASE
                        WHEN i.src_talent_description LIKE '%ALMAS%'
                        THEN 'ANDRADE ''CIEN'' ALMAS'
                        WHEN i.src_talent_description = 'RANDOM'
                        THEN 'RONDA ROUSEY'
                        WHEN i.src_talent_description IN ('HARDY''S',
                                                          'HARDY BOYZ')
                        THEN 'HARDY BOYZ'
                        WHEN i.src_talent_description LIKE '%ELIAS%'
                        THEN 'ELIAS'
                        WHEN i.src_talent_description LIKE '%BIG E%'
                        THEN 'BIG E'
                        WHEN i.src_talent_description LIKE '%APOLLO%'
                        THEN 'APOLLO CREWS'
                        ELSE i.src_talent_description
                    END                      AS talent_description,
                    SUM(s.src_units_ordered) AS units,
                    SUM(s.demand_sales_$)    AS revenue
                FROM
                    {{source('fds_cpg','aggr_cpg_daily_sales')}} s,
                    {{source('fds_cpg','dim_cpg_item')}} i
                WHERE
                    week_start = TRUNC(date_trunc('week',(CURRENT_DATE - 42)))
                AND s.dim_item_id = i.dim_item_id
                AND s.src_units_ordered > 0
                AND src_talent_description NOT IN ('WWE',
                                                   'RAW',
                                                   'WRESTLEMANIA',
                                                   'SMACKDOWN',
                                                   'NXT',
                                                   'WOMEN''S DIVISION',
                                                   'DIVA',
                                                   'EVOLUTION WOMEN''S PPV',
                                                   'TAPOUT',
                                                   'WWE NETWORK',
                                                   'ECW',
                                                   'BIRDIEBEE',
                                                   'LEGENDS',
                                                   '205 Live',
                                                   'PPV',
                                                   'BLANK -SALES RPT CODE 1 41/S1')
                GROUP BY
                    1,2)) b
ON
    a.talent_description = b.talent_description
LEFT JOIN
    (
        SELECT
            talent_description,
            src_style_description AS top_product
        FROM
            (
                SELECT
                    CASE
                        WHEN i.src_talent_description LIKE '%ALMAS%'
                        THEN 'ANDRADE ''CIEN'' ALMAS'
                        WHEN i.src_talent_description = 'RANDOM'
                        THEN 'RONDA ROUSEY'
                        WHEN i.src_talent_description IN ('HARDY''S',
                                                          'HARDY BOYZ')
                        THEN 'HARDY BOYZ'
                        WHEN i.src_talent_description LIKE '%ELIAS%'
                        THEN 'ELIAS'
                        WHEN i.src_talent_description LIKE '%BIG E%'
                        THEN 'BIG E'
                        WHEN i.src_talent_description LIKE '%APOLLO%'
                        THEN 'APOLLO CREWS'
                        ELSE i.src_talent_description
                    END AS talent_description,
                    i.src_style_description,
                    SUM(s.demand_sales_$)                                                AS revenue,
                    row_number() over (partition BY talent_description ORDER BY revenue DESC) AS rn
                FROM
                    {{source('fds_cpg','aggr_cpg_daily_sales')}} s,
                    {{source('fds_cpg','dim_cpg_item')}} i
                WHERE
                    s.dim_item_id = i.dim_item_id
                AND TRUNC(date_trunc('week',CAST(CAST(s.date_key AS VARCHAR(10)) AS DATE))) = TRUNC
                    (date_trunc('week',(CURRENT_DATE - 35)))
                GROUP BY
                    talent_description,
                    i.src_style_id,
                    i.src_style_description
                ORDER BY
                    talent_description)
        WHERE
            rn = 1) c
ON
    a.talent_description = c.talent_description