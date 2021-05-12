{{
  config({
		'schema': 'fds_cpg',
		"materialized": 'table','tags': "Phase 5B","persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": 'grant select on {{this}} to public'
  })
}}
SELECT
    dataset.report_month,
    initcap(dt.mth_nm)          AS MONTH,
    dt.cal_year                 AS YEAR,
    UPPER(dt.cal_year_qtr_desc) AS quarter,
    dataset.item_id,
    dataset.item_description,
    dataset.royalty_code,
    UPPER(dataset.source) AS source,
    dataset.currency_type,
    dataset.shipped_quantity,
    dataset.shipped_sales,
    dataset.returned_quantity,
    dataset.returned_sales,
    dataset.refunded_quantity,
    dataset.refunded_sales,
    ('DBT_' || TO_CHAR(SYSDATE, 'YYYY_MM_DD_HH_MI_SS') || '_5B') AS etl_batch_id,
    'bi_dbt_user_prd'                                            AS etl_insert_user_id,
    SYSDATE                                                      AS etl_insert_rec_dttm,
    CAST(NULL AS VARCHAR(15))                                    AS etl_update_user_id,
    CAST(NULL AS TIMESTAMP)                                      AS etl_update_rec_dttm
FROM
    {{ref('intm_cpg_monthly_royalty_rpt')}} dataset
JOIN
    {{source('cdm','dim_date')}} dt
ON
    dataset.report_month = dt.full_date