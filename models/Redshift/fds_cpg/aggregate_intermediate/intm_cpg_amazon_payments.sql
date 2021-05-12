{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    UPPER(source)                                              AS source ,
    UPPER(currency_code)                                       AS currency_type ,
    TRUNC(date_trunc('month', to_date(as_on_date,'YYYYMMDD'))) AS report_month ,
    sku                                                        AS item_id ,
    ABS(COALESCE(COUNT(DISTINCT quantity),0))                  AS shipped_quantity ,
    ABS(COALESCE(SUM(
        CASE
            WHEN type_english='adjustment'
            THEN other
            ELSE (product_sales+promotional_rebates)
        END),0.000)) AS shipped_sales
FROM
    {{source('hive_udl_cpg','raw_amazon_monthly_payments')}}
WHERE
    LOWER(type_english) IN ('order',
                            'adjustment')
AND sku IS NOT NULL
GROUP BY
    1,2,3,4