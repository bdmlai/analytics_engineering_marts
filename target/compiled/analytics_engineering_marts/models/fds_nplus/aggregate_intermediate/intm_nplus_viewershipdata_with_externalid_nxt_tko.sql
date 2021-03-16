

select  src_fan_id, stream_id, stream_start_dttm as min_time, stream_end_dttm as max_time, time_spent, external_id, stream_type_cd, 
         stream_device_platform,content_wwe_id, content_duration, live_start_schedule as live_start,
         live_end_schedule as live_end, first_stream, _same_day, _3day_dt, _7day_dt, _30day_dt       
        from "entdwdb"."fds_nplus"."fact_daily_content_viewership" 
        where external_id in (select network_id from __dbt__CTE__intm_nplus_content_nxt_tko)
        and trunc(live_start_schedule) in (select premiere_date from __dbt__CTE__intm_nplus_content_nxt_tko)