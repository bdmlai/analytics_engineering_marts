{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *,
(select count(distinct stream_id) from {{ref('intm_nplus_viewership_data_with_externalid')}} b 
where a.external_id = b.external_id and b.min_time < a.time_interval and b.max_time > a.time_interval
) as streams_count,
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewership_data_with_externalid')}} c where a.external_id = c.external_id 
and (c.min_time <= a.time_interval) 
) as cumulative_unique_user,
(select users_added from {{ref('intm_nplus_ppv_streams_users_metrics')}} b where a.external_id = b.external_id
  and a.time_interval=b.time_interval
)as users_added,
	
(select count(distinct src_fan_id) from {{ref('intm_nplus_viewership_data_with_externalid')}}  
where a.external_id = external_id and min_time <= a.time_interval and max_time >= a.time_interval  
) as total_user,

(select count(distinct src_fan_id) from {{ref('intm_nplus_viewership_data_with_externalid')}}  
where a.external_id = external_id and min_time <= a.prev_time_interval and max_time >= a.time_interval  
) as previous_seg_users

from {{ref('intm_nplus_viewership_sequence_generator')}} a