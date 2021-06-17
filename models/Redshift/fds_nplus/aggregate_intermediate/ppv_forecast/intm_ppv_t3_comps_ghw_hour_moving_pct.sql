{{
  config({
		"materialized": 'table',
		"schema": 'dt_prod_support'
  })
}}
SELECT sum(ag_sum_pct) OVER (
    partition by adds_day_of_week
    ORDER BY adds_time,
      adds_day_of_week rows unbounded preceding
  ) ag_moving_pct,
  adds_time,
  adds_day_of_week
FROM {{ref('intm_ppv_t1_hourly_comps_pct')}}