{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    (
        CASE
            WHEN shp_rtn.source IS NULL
            THEN rfnd.src_sys_cd
            ELSE shp_rtn.source
        END) AS source ,
    (
        CASE
            WHEN shp_rtn.currency_type IS NULL
            THEN rfnd.src_currency_code_from
            ELSE shp_rtn.currency_type
        END) AS currency_type ,
    (
        CASE
            WHEN shp_rtn.report_month IS NULL
            THEN rfnd.report_month
            ELSE shp_rtn.report_month
        END) AS report_month ,
    (
        CASE
            WHEN shp_rtn.item_id IS NULL
            THEN rfnd.src_item_id
            ELSE shp_rtn.item_id
        END) AS item_id ,
    (
        CASE
            WHEN shp_rtn.item_description IS NULL
            THEN rfnd.src_item_description
            ELSE shp_rtn.item_description
        END) AS item_description ,
    (
        CASE
            WHEN shp_rtn.royalty_code IS NULL
            THEN rfnd.src_royalty_name
            ELSE shp_rtn.royalty_code
        END)                                AS royalty_code ,
    COALESCE(shp_rtn.shipped_quantity, 0)   AS shipped_quantity ,
    COALESCE(shp_rtn.shipped_sales, 0.000)  AS shipped_sales ,
    COALESCE(shp_rtn.returned_quantity,0)   AS returned_quantity ,
    COALESCE(shp_rtn.returned_sales, 0.000) AS returned_sales ,
    COALESCE(rfnd.refunded_quantity,0)      AS refunded_quantity ,
    COALESCE(rfnd.refunded_sales, 0.000)    AS refunded_sales
FROM
    {{ref('intm_cpg_shopify_shipment_returns')}} shp_rtn
FULL JOIN
    {{ref('intm_cpg_shopify_refunds')}} rfnd
ON
    shp_rtn.source=rfnd.src_sys_cd
AND shp_rtn.report_month=rfnd.report_month
AND shp_rtn.item_id=rfnd.src_item_id
AND shp_rtn.currency_type=rfnd.src_currency_code_from