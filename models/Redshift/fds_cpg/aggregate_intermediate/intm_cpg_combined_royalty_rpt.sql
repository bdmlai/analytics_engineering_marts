{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *
FROM
    {{ref('intm_cpg_clientbase_royalty_rpt')}}
UNION ALL
SELECT
    *
FROM
    {{ref('intm_cpg_radial_royalty_rpt')}}
UNION ALL
SELECT
    *
FROM
    {{ref('intm_cpg_amazon_royalty_rpt')}}
