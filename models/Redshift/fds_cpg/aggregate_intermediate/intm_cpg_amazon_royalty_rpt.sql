{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    (
        CASE
            WHEN a.source IS NULL
            THEN b.source
            ELSE a.source
        END) AS source ,
    (
        CASE
            WHEN a.currency_type IS NULL
            THEN b.currency_type
            ELSE a.currency_type
        END) AS currency_type ,
    (
        CASE
            WHEN a.report_month IS NULL
            THEN b.report_month
            ELSE a.report_month
        END) AS report_month ,
    (
        CASE
            WHEN a.item_id IS NULL
            THEN b.item_id
            ELSE a.item_id
        END)                          AS item_id ,
    COALESCE(a.shipped_quantity, 0)   AS shipped_quantity ,
    COALESCE(a.shipped_sales, 0.000)  AS shipped_sales ,
    0                                 AS returned_quantity ,
    0.000                             AS returned_sales ,
    COALESCE(b.refunded_quantity,0)   AS refunded_quantity ,
    COALESCE(b.refunded_sales, 0.000) AS refunded_sales
FROM
    {{ref('intm_cpg_amazon_payments')}} a
FULL JOIN
    {{ref('intm_cpg_amazon_refunds')}} b
ON
    a.source=b.source
AND a.currency_type=b.currency_type
AND a.report_month=b.report_month
AND a.item_id=b.item_id