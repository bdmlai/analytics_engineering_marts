{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    'CLIENTBASE' AS source ,
    currency_type ,
    report_month ,
    item_id ,
    ABS(COALESCE(COUNT(*),0))           AS returned_quantity ,
    ABS(COALESCE(SUM(merch_net),0.000)) AS returned_sales
FROM
    {{source('hive_udl_cpg','clientbase_monthly_returns')}}
WHERE
    item_id IS NOT NULL
GROUP BY
    1,2,3,4