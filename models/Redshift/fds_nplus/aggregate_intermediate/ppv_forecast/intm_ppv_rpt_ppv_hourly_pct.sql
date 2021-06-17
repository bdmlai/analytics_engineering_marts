{{ config(materialized = 'table',schema='dt_prod_support',
            enabled = true, 
            post_hook = "drop table dt_prod_support.intm_ppv_t2_ghw_total;
								drop table dt_prod_support.intm_ppv_t2_ghw_pct;
								drop table dt_prod_support.intm_ppv_forcst_ghw_pct_avg;
								drop table dt_prod_support.intm_ppv_t1_hourly_comps_pct;
								drop table dt_prod_support.intm_ppv_t3_comps_ghw_hour_moving_pct;
								drop table dt_prod_support.intm_ppv_t3_comps_ghw_hour_pct;" ) }}

SELECT a.adds_day_of_week,
        a.adds_time,
        a.ag_sum_pct pct,
        b.day_pct d2_pct,
        null gwh_pct,
        null gwh_pct_day
FROM {{ref('intm_ppv_t1_hourly_comps_pct')}} a
        JOIN {{ref('intm_ppv_t3_comps_ghw_hour_pct')}} b ON a.adds_day_of_week = b.adds_day_of_week
        AND a.adds_time = b.adds_time
UNION
SELECT null,
        null,
        null,
        null,
        tot_pct,
        adds_day_of_week
FROM {{ref('intm_ppv_forcst_ghw_pct_avg')}}