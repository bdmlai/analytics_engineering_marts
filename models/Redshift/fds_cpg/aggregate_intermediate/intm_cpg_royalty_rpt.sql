{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    a.source ,
    a.currency_type ,
    a.report_month ,
    a.item_id ,
    b.item_description ,
    b.royalty_code ,
    a.shipped_quantity ,
    a.shipped_sales ,
    (-a.returned_quantity) AS returned_quantity ,
    (-a.returned_sales)    AS returned_sales ,
    (-a.refunded_quantity) AS refunded_quantity ,
    (-a.refunded_sales)    AS refunded_sales
FROM
    {{ref('intm_cpg_combined_royalty_rpt')}} a
LEFT JOIN
    {{ref('intm_cpg_src_item_detail')}} b
ON
    a.item_id=b.item_id