{{
  config({
		"schema": 'dt_prod_support',
		"materialized": 'table','tags': "rpt_nplus_daily_live_streams", 
		'post-hook': 'grant select on {{ this }} to public'
  })
}}
select distinct b.src_fan_id,a.external_id,a.time_interval,a.prev_time_interval
 from {{ref('intm_nplus_daily_viewership_sequence_generator_live')}} a
 join {{ref('intm_nplus_timeintervals_minmaxtime_live')}} b on a.external_id=b.external_id
  and a.time_interval=b.time_interval
  and a.prev_time_interval=b.prev_time_interval
 where  b.min_time < a.time_interval and  b.max_time >= a.time_interval