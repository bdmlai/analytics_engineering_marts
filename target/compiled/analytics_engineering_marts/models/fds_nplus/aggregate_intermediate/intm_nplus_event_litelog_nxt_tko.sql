

select a.*,
b.* from __dbt__CTE__intm_nplus_duration_nxt_tko a 
left join __dbt__CTE__intm_nplus_lite_log_est_nxt_tko b
on a.live_start_schedule<= b.in_time_est
and substring(trim(a.live_end_schedule), 1, 16) >= substring(trim(b.out_time_est), 1, 16)