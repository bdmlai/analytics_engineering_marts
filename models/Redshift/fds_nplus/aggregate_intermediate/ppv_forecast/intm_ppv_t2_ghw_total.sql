{{
  config({
		"materialized": 'table',
		"schema": 'dt_prod_support'
  })
}}
SELECT sum(ghw_avg) tot_sum
FROM (
    SELECT avg(ghw_sum) ghw_avg,
      adds_day_of_week
    FROM (
        SELECT sum(paid_adds) + sum(trial_adds) ghw_sum,
          adds_day_of_week,
          event_name
        FROM {{ref('intm_ppv_final_table')}}
        WHERE event_type NOT IN ('current_ppv', 'comp3')
        GROUP BY adds_day_of_week,
          event_name
      )
    GROUP BY 2
  )