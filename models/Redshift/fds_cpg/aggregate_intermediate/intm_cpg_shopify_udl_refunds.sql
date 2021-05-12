{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *
FROM
    (
        SELECT
            shopify_refund_id,
            shopify_refund_line_id,
            refund_line_subtotal_amount,
            shop_name,
            as_on_date,
            row_number() over (partition BY shopify_refund_id,shopify_refund_line_id,
            refund_line_subtotal_amount,shop_name ORDER BY as_on_date DESC) rnk
        FROM
            {{source('hive_udl_cpg','cpg_shopify_daily_order_refunds')}}
        WHERE
            shop_name = 'cpgusshopify')
WHERE
    rnk=1