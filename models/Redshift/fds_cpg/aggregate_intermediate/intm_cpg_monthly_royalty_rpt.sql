{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *
FROM
    {{ref('intm_cpg_royalty_rpt')}}
UNION ALL
SELECT
    *
FROM
    {{ref('intm_cpg_shopify_royalty_rpt')}}