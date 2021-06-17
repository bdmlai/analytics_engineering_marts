{{
  config({
		"materialized": 'table',
		"schema": 'dt_prod_support'
  })
}}
select CASE
                WHEN adds_day_of_week = 'Friday' THEN (
                        SELECT sum(tot_pct)
                        FROM {{ref('intm_ppv_forcst_ghw_pct_avg')}}
                        WHERE adds_day_of_week NOT IN ('Friday', 'Saturday', 'Sunday')
                ) + (
                        SELECT sum(tot_pct)
                        FROM {{ref('intm_ppv_forcst_ghw_pct_avg')}}
                        WHERE adds_day_of_week = 'Friday'
                ) * (
                        SELECT ag_moving_pct
                        FROM {{ref('intm_ppv_t3_comps_ghw_hour_moving_pct')}}
                        WHERE adds_time = a.adds_time
                                AND adds_day_of_week = a.adds_day_of_week
                )
                WHEN adds_day_of_week = 'Saturday' THEN (
                        SELECT sum(tot_pct)
                        FROM {{ref('intm_ppv_forcst_ghw_pct_avg')}}
                        WHERE adds_day_of_week NOT IN ('Saturday', 'Sunday')
                ) + (
                        SELECT sum(tot_pct)
                        FROM {{ref('intm_ppv_forcst_ghw_pct_avg')}}
                        WHERE adds_day_of_week = 'Saturday'
                ) * (
                        SELECT ag_moving_pct
                        FROM {{ref('intm_ppv_t3_comps_ghw_hour_moving_pct')}}
                        WHERE adds_time = a.adds_time
                                AND adds_day_of_week = a.adds_day_of_week
                )
                WHEN adds_day_of_week = 'Sunday' THEN (
                        SELECT sum(tot_pct)
                        FROM {{ref('intm_ppv_forcst_ghw_pct_avg')}}
                        WHERE adds_day_of_week NOT IN ('Sunday')
                ) + (
                        SELECT sum(tot_pct)
                        FROM {{ref('intm_ppv_forcst_ghw_pct_avg')}}
                        WHERE adds_day_of_week = 'Sunday'
                ) * (
                        SELECT ag_moving_pct
                        FROM {{ref('intm_ppv_t3_comps_ghw_hour_moving_pct')}}
                        WHERE adds_time = a.adds_time
                                AND adds_day_of_week = a.adds_day_of_week
                )
        END day_pct,
        adds_day_of_week,
        adds_time
FROM {{ref('intm_ppv_t1_hourly_comps_pct')}} a