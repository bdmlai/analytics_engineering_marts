{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    (
        CASE
            WHEN shp.src_sys_cd IS NULL
            THEN rtn.src_sys_cd
            ELSE shp.src_sys_cd
        END) AS source ,
    (
        CASE
            WHEN shp.src_currency_code_from IS NULL
            THEN rtn.src_currency_code_from
            ELSE shp.src_currency_code_from
        END) AS currency_type ,
    (
        CASE
            WHEN shp.report_month IS NULL
            THEN rtn.report_month
            ELSE shp.report_month
        END) AS report_month ,
    (
        CASE
            WHEN shp.src_item_id IS NULL
            THEN rtn.src_item_id
            ELSE shp.src_item_id
        END) AS item_id ,
    (
        CASE
            WHEN shp.src_item_description IS NULL
            THEN rtn.src_item_description
            ELSE shp.src_item_description
        END) AS item_description ,
    (
        CASE
            WHEN shp.src_royalty_name IS NULL
            THEN rtn.src_royalty_name
            ELSE shp.src_royalty_name
        END)                            AS royalty_code ,
    COALESCE(shp.shipped_quantity, 0)   AS shipped_quantity ,
    COALESCE(shp.shipped_sales, 0.000)  AS shipped_sales ,
    COALESCE(rtn.returned_quantity,0)   AS returned_quantity ,
    COALESCE(rtn.returned_sales, 0.000) AS returned_sales
FROM
    {{ref('intm_cpg_shopify_shipments')}} shp
FULL JOIN
    {{ref('intm_cpg_shopify_returns')}} rtn
ON
    shp.src_sys_cd=rtn.src_sys_cd
AND shp.report_month=rtn.report_month
AND shp.src_item_id=rtn.src_item_id
AND shp.src_currency_code_from=rtn.src_currency_code_from