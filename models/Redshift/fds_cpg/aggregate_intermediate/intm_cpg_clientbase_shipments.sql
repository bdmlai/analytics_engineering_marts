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
    ABS(COALESCE(SUM(qty_shipped),0))                 AS shipped_quantity ,
    ABS(COALESCE(SUM(net_merchandise_exc_vat),0.000)) AS shipped_sales
FROM
    {{source('udl_pii','restricted_cpg_clientbase_monthly_shipment')}}
WHERE
    item_id IS NOT NULL
GROUP BY
    1,2,3,4