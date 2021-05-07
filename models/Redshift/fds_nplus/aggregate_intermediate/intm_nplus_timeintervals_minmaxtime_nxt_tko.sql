{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.premiere_date,a.external_id,a.time_interval,a.prev_time_interval,b.src_fan_id,b.min_time,b.max_time 
 from {{ref('intm_nplus_viewership_sequence_generator_nxt_tko')}} a
 inner join  {{ref('intm_nplus_viewershipdata_with_externalid_nxt_tko')}} b
 on a.external_id=b.external_id