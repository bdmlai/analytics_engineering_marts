

select a.wwe_id as external_id,a.series_episode,a.content_duration,a.live_start_schedule, a.live_end_schedule,
b.* from __dbt__CTE__intm_nplus_duration_205_Live a 
left join __dbt__CTE__intm_nplus_lite_log_est_live b
on a.live_start_schedule<= b.in_time_est
and substring(trim(a.live_end_schedule), 1, 16) >= substring(trim(b.out_time_est), 1, 16)