{{
  config({
		"materialized": 'table',
		"schema": 'dt_prod_support'
  })
}}
SELECT (ag_hour * 1.00) / ag_sum ag_sum_pct,
    a.adds_day_of_week,
    adds_time
FROM (
        SELECT ag_hour,
            adds_day_of_week,
            adds_time
        FROM (
                SELECT avg(paid_adds) + avg(trial_adds) ag_hour,
                    adds_time,
                    adds_day_of_week
                FROM {{ref('intm_ppv_final_table_up')}}
                WHERE adds_day_of_week IN ('Friday', 'Saturday', 'Sunday')
                    AND event_type NOT IN ('current_ppv', 'comp3') --!='comp3'
                GROUP BY adds_time,
                    adds_day_of_week
            )
    ) a
    LEFT JOIN (
        SELECT sum(ag_hour) ag_sum,
            adds_day_of_week
        FROM (
                SELECT avg(paid_adds) + avg(trial_adds) ag_hour,
                    adds_day_of_week
                FROM {{ref('intm_ppv_final_table_up')}}
                WHERE adds_day_of_week IN ('Friday', 'Saturday', 'Sunday')
                    AND event_type NOT IN ('current_ppv', 'comp3') --!='comp3'
                GROUP BY adds_time,
                    adds_day_of_week
            )
        GROUP BY 2
    ) b ON a.adds_day_of_week = b.adds_day_of_week