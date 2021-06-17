{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT *,
        (((ghw_adds_tillnow1+currentday_adds_tillnow)/d2_pct1)*(d2_pct2-d2_pct1))+currentday_adds_tillnow currentday_adds_estimate,
         (((ghw_adds_tillnow1+currentday_adds_tillnow)/d2_pct1)*(1-d2_pct1))+(currentday_adds_tillnow+ghw_adds_tillnow2) weekend_adds_estimate
FROM {{ref('intm_ppv_estimates_first_part')}}