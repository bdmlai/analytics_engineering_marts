{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.*,b.* from {{ref('intm_nplus_content_duration')}} a 
left join {{ref('intm_nplus_lite_log_est')}} b
on a.live_start_schedule<= b.in_time_est
and substring(trim(a.live_end_schedule), 1, 16) >= substring(trim(b.out_time_est), 1, 16)