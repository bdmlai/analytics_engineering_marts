{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'table',
		"tags":["rpt_nplus_daily_live_streams","rpt_nplus_daily_milestone_complete_rates"],
		'post-hook': 'grant select on {{ this }} to public'
  })
}}

select  src_fan_id, stream_id, stream_start_dttm as min_time, stream_end_dttm as max_time, time_spent, external_id, stream_type_cd, stream_device_platform,
        content_wwe_id, content_duration, live_start_schedule as live_start,live_end_schedule as live_end, first_stream, _same_day, _3day_dt, _7day_dt, _30day_dt 
        from {{source('fds_nplus','fact_daily_content_viewership')}} where external_id in (select wwe_id from {{ref('intm_nplus_content_Live')}})
		and trunc(live_start_schedule) in (select airdate from {{ref('intm_nplus_content_Live')}})