{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    sd.src_sys_cd,
    sd.src_currency_code_from,
    itm1.src_item_id,
    itm1.src_item_description,
    itm1.src_royalty_name,
    TRUNC(date_trunc('month', to_date(COALESCE(sd.ship_date_id,sd.order_date_id), 'YYYYMMDD'))) AS
              report_month,
    0::INT                                                   AS refunded_quantity,
    (COALESCE(SUM(rfnd.refund_line_subtotal_amount),0) * -1) AS refunded_sales
FROM
    {{source('fds_cpg','fact_cpg_sales_detail')}} sd
JOIN
    {{ref('intm_cpg_shopify_udl_refunds')}} rfnd
ON
    sd.src_order_number = rfnd.shopify_refund_id
AND sd.src_sequence_number = rfnd.shopify_refund_line_id
AND sd.src_sys_cd = rfnd.shop_name
JOIN
    {{source('fds_cpg','dim_cpg_item')}} itm
ON
    sd.dim_item_id = itm.dim_item_id
JOIN
    (
        SELECT DISTINCT
            src_item_id,
            src_item_description,
            dim_business_unit_id,
            src_royalty_name
        FROM
            {{source('fds_cpg','dim_cpg_item')}}
        WHERE
            active_flag='Y'
        AND LOWER(trim(src_item_id)) <> 'undefined') itm1
ON
    itm.src_item_id = itm1.src_item_id
AND itm.dim_business_unit_id = itm1.dim_business_unit_id
WHERE
    COALESCE(sd.ship_date_id,sd.order_date_id) >= 20210301
AND TRUNC(date_trunc('month', to_date(COALESCE(sd.ship_date_id,sd.order_date_id), 'YYYYMMDD'))) <
    TRUNC(date_trunc('month', CURRENT_DATE))
AND sd.src_sys_cd = 'cpgusshopify'
AND ABS(COALESCE(sd.src_units_shipped,0))> 0
GROUP BY
    1,2,3,4,5,6,7