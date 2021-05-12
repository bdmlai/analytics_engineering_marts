{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    UPPER(source)                                               AS source ,
    UPPER(currency_code)                                        AS currency_type ,
    TRUNC(date_trunc('month', to_date(as_on_date,'YYYYMMDD')))  AS report_month ,
    sku                                                         AS item_id ,
    ABS(COALESCE(COUNT(DISTINCT quantity),0))                   AS refunded_quantity ,
    ABS(COALESCE(SUM(product_sales+promotional_rebates),0.000)) AS refunded_sales
FROM
    {{source('hive_udl_cpg','raw_amazon_monthly_payments')}}
WHERE
    LOWER(type_english) IN ('refund')
AND sku IS NOT NULL
GROUP BY
    1,2,3,4