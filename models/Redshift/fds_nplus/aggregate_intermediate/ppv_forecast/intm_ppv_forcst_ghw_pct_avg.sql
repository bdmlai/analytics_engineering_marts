{{
  config({
		"materialized": 'table',
		"schema": 'dt_prod_support'
  })
}}
SELECT ((tot_pct) * 1.00) tot_pct,
  adds_day_of_week
FROM (
    SELECT adds_day_of_week,
      tot_pct
    FROM {{ ref('intm_ppv_t2_ghw_pct')}}
  ) a