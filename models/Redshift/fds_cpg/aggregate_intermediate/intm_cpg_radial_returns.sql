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
    ABS(COALESCE(SUM(units_returned),0)) AS returned_quantity ,
    ABS(COALESCE(SUM(merch_net),0.000))  AS returned_sales
FROM
    {{source('hive_udl_cpg','radial_monthly_order_return')}}
WHERE
    item_id IS NOT NULL
GROUP BY
    1,2,3,4