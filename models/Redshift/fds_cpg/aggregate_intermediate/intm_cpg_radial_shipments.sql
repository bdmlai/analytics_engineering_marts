{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    'RADIAL' AS source ,
    currency_type ,
    report_month ,
    item_id ,
    ABS(COALESCE(SUM(units_shipped),0)) AS shipped_quantity ,
    ABS(COALESCE(SUM(merch_net),0.000)) AS shipped_sales
FROM
    {{source('hive_udl_cpg','radial_monthly_order_ship')}}
WHERE
    item_id IS NOT NULL
GROUP BY
    1,2,3,4