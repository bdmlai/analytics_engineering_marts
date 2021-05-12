{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    'RADIAL' AS source ,
    'USD'    AS currency_type ,
    report_month ,
    sku                                     AS item_id ,
    0                                       AS refunded_quantity ,
    ABS(COALESCE(SUM(product_amount),0.00)) AS refunded_sales
FROM
    {{source('hive_udl_cpg','radial_monthly_payment_refund')}}
WHERE
    return1_return2=1
AND sku IS NOT NULL
GROUP BY
    1,2,3,4