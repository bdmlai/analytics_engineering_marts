{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'table','tags': "rpt_nplus_daily_live_streams", 
		'post-hook': 'grant select on {{ this }} to public'
  })
}}
select a.airdate,a.external_id,a.time_interval,a.prev_time_interval,b.src_fan_id,b.min_time,b.max_time 
 from {{ref('intm_nplus_daily_viewership_sequence_generator_live')}} a
 inner join  {{ref('intm_nplus_viewershipdata_with_externalid_live')}} b
 on a.external_id=b.external_id