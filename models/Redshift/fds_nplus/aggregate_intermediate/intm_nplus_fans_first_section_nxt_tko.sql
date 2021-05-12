{{
  config({
		"materialized": 'ephemeral'
  })
}}
select distinct b.src_fan_id,a.external_id,a.time_interval,a.prev_time_interval
 from {{ref('intm_nplus_viewership_sequence_generator_nxt_tko')}} a
 join {{ref('intm_nplus_timeintervals_minmaxtime_nxt_tko')}} b on a.external_id=b.external_id
  and a.time_interval=b.time_interval
  and a.prev_time_interval=b.prev_time_interval
 where  b.min_time < a.time_interval and  b.max_time >= a.time_interval