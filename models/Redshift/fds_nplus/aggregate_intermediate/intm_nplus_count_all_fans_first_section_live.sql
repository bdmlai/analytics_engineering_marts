{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'table','tags': "rpt_nplus_daily_live_streams", 
		'post-hook': 'grant select on {{ this }} to public'
  })
}}
select count(src_fan_id) as curr_cnt,external_id,time_interval,prev_time_interval
 from {{ref('intm_nplus_fans_first_section_live')}}
 group by external_id,time_interval,prev_time_interval