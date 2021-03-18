

with __dbt__CTE__intm_nplus_content_classification_title as (


select distinct premiere_date, network_id, episode_nm as title from "entdwdb"."cdm"."dim_content_classification_title"
 where series_group = 'WWE PPV' and (premiere_date >= current_date-7 and  premiere_date <=current_date) and right(episode_nm,4) != '(ES)' 
   and network_id like '%ppv%'
),  __dbt__CTE__intm_nplus_content_duration as (


select * from __dbt__CTE__intm_nplus_content_classification_title a 
left join
(select distinct external_id, content_duration, live_start_schedule, live_end_schedule 
from "entdwdb"."fds_nplus"."fact_daily_content_viewership" 
where external_id in (select network_id from __dbt__CTE__intm_nplus_content_classification_title) 
and trunc(live_start_schedule) in (select premiere_date from __dbt__CTE__intm_nplus_content_classification_title)
)b
on a.network_id = b.external_id
and a.premiere_date=trunc(b.live_start_schedule)
),  __dbt__CTE__intm_nplus_litelog_est_time_diff as (


select distinct c.showdbid,c.min_inpoint,d.start_time_eastern,
dateadd(hr,12,(airdate || ' ' || substring(trim(c.min_inpoint), 1, 8))::timestamp) as hr_min_inpoint,
         datediff(sec,  dateadd(hr,12,(airdate || ' ' || substring(trim(c.min_inpoint), 1, 8))::timestamp), 
         dateadd(hr,12,(d.air_date || ' ' || substring(trim(d.start_time_eastern), 1, 8))::timestamp))
 as est_time_diff_1,
  (round(est_time_diff_1/900,0)*900)::integer as est_time_diff
from 
(select distinct a.showdbid, a.min_inpoint, b.airdate, b.logname
from
(select showdbid, min(inpoint) as min_inpoint
from "entdwdb"."udl_nplus"."raw_post_event_log"
where lower(title) not like '%%kickoff%%' and lower(title) not like '%%nxt%%' and lower(title) not like '%%raw%%'
        and lower(title) not like '%%smackdown%%' and lower(title) not like '%%205 live%%' and lower(title) not like '%%king%%'
        and lower(title) not like '%%tribute%%' and lower(title) not like '%%mixed match%%' and issegmentmarker='TRUE'
        and upper(segmenttype) in ('MATCH','BACKSTAGE ON CAMERA','IN-ARENA NON-MATCH','ON CAMERA','TALK SHOW') 
        and airdate in (select premiere_date from __dbt__CTE__intm_nplus_content_duration)
 and showdbid is not null and showdbid <> 0 and inpoint is not null 
and inpoint <> ' ' and segmenttype is not null 
group by 1) a
join "entdwdb"."udl_nplus"."raw_post_event_log" b
on a.showdbid = b.showdbid and a.min_inpoint = b.inpoint) c
join "entdwdb"."udl_emm"."emm_weekly_log_reference" d 
on c.airdate = d.air_date and
lower(trim(c.logname)) = lower(trim(d.logname))
where airdate in ( select premiere_date from __dbt__CTE__intm_nplus_content_duration)
 and d.start_time_eastern is not null and d.start_time_eastern not in ('0', ' ')
),  __dbt__CTE__intm_nplus_lite_log_est as (


select a.showdbid, title as titles, subtitle, episodenumber, airdate, inpoint, outpoint,
case when  abs(b.est_time_diff)/60 <=15 then  dateadd(hr,12,(airdate || ' ' || substring(trim(inpoint), 1, 8))::timestamp)
else 
(dateadd(hr, 12, (dateadd(sec, b.est_time_diff, (airdate || ' ' || substring(trim(inpoint), 1, 8))::timestamp)))) end as inpoint_24hr_est, 
--dateadd(sec, 30, inpoint_24hr_est) as in_time_est,
inpoint_24hr_est as in_time_est,
(((dateadd(sec, (((substring(duration, 1, 2))::int * 3600) + ((substring(duration, 4, 2))::int * 60) 
+ ((substring(duration, 7, 2))::int) ), inpoint_24hr_est)))::timestamp) as out_time_est,
segmenttype, comment as milestone, matchtype, talentactions, move, finishtype, additionaltalent, venuelocation, venuename,  
upper(right(trim(venuelocation),2)) as state, rank() over (partition by airdate order by inpoint) as seg_num
FROM "entdwdb"."udl_nplus"."raw_post_event_log" a 
join __dbt__CTE__intm_nplus_litelog_est_time_diff b on a.showdbid = b.showdbid
where  airdate is not null and inpoint is not null and duration is not null
and inpoint <> ' ' and duration <> ' ' 
and lower(title) not like '%%kickoff%%' and lower(title) not like '%%nxt%%' and lower(title) not like '%%raw%%'
        and lower(title) not like '%%smackdown%%' and lower(title) not like '%%205 live%%' and lower(title) not like '%%king%%'
        and lower(title) not like '%%tribute%%' and lower(title) not like '%%mixed match%%' and issegmentmarker='TRUE'
        and upper(segmenttype) in ('MATCH','BACKSTAGE ON CAMERA','IN-ARENA NON-MATCH','ON CAMERA','TALK SHOW')
),  __dbt__CTE__intm_nplus_event_lite_log_data as (

select a.*,b.* from __dbt__CTE__intm_nplus_content_duration a 
left join __dbt__CTE__intm_nplus_lite_log_est b
on a.live_start_schedule<= b.in_time_est
and substring(trim(a.live_end_schedule), 1, 16) >= substring(trim(b.out_time_est), 1, 16)
),  __dbt__CTE__intm_nplus_viewership_data_with_externalid as (

select  src_fan_id, stream_id, stream_start_dttm as min_time, stream_end_dttm as max_time, time_spent, external_id, stream_type_cd, 
         stream_device_platform,content_wwe_id, content_duration, live_start_schedule as live_start,
         live_end_schedule as live_end, first_stream, _same_day, _3day_dt, _7day_dt, _30day_dt       
        from "entdwdb"."fds_nplus"."fact_daily_content_viewership"
         where external_id in (select network_id from __dbt__CTE__intm_nplus_event_lite_log_data)
        and trunc(live_start_schedule) in (select premiere_date from __dbt__CTE__intm_nplus_event_lite_log_data)
),  __dbt__CTE__intm_nplus_max_cum_users as (

select *, (max(cumulative_unique_user) over (partition by external_id, premiere_date)) as max_cum_unique_users
from "entdwdb"."fds_nplus"."rpt_nplus_daily_ppv_streams" where premiere_date >= current_date-7
),  __dbt__CTE__intm_nplus_cg_ppv_streams_v2 as (

select a.*, (max_cum_unique_users::float/ prev_max_cum_unique_users::float-1) as pct_change
from __dbt__CTE__intm_nplus_max_cum_users a
left join
(select premiere_date, external_id, (lag(cumulative_unique_user,1) over (order by premiere_date asc,time_interval asc)) prev_max_cum_unique_users
 from
(select premiere_date, external_id, min(time_interval) as time_interval,max(cumulative_unique_user) as cumulative_unique_user
 from __dbt__CTE__intm_nplus_max_cum_users group by 1,2))b
on a.external_id = b.external_id and a.premiere_date = b.premiere_date
),  __dbt__CTE__intm_nplus_content_205_Live as (


select distinct airdate,episodenumber, 'wwetof0'+ lpad(episodenumber,3,'0') as wwe_id, '205 LIVE '+ lpad(episodenumber,3,'0') as series_episode 
from "entdwdb"."udl_nplus"."raw_post_event_log"
where (title like '%205%') 
and (airdate >= current_date-7 and  airdate <=current_date)
order by episodenumber
),  __dbt__CTE__intm_nplus_viewershipdata_with_externalid_live as (


select  src_fan_id, stream_id, stream_start_dttm as min_time, stream_end_dttm as max_time, time_spent, external_id, stream_type_cd, stream_device_platform,
        content_wwe_id, content_duration, live_start_schedule as live_start,live_end_schedule as live_end, first_stream, _same_day, _3day_dt, _7day_dt, _30day_dt 
        from "entdwdb"."fds_nplus"."fact_daily_content_viewership" where external_id in (select wwe_id from __dbt__CTE__intm_nplus_content_205_Live)
		and trunc(live_start_schedule) in (select airdate from __dbt__CTE__intm_nplus_content_205_Live)
),  __dbt__CTE__intm_nplus_max_cum_users_live as (

select *, (max(cumulative_unique_user) over (partition by external_id, airdate)) as max_cum_unique_users
from "entdwdb"."fds_nplus"."rpt_nplus_daily_live_streams" where airdate >= current_date-7
),  __dbt__CTE__intm_nplus_cg_live_streams_v2 as (

select a.*, (max_cum_unique_users::float/ prev_max_cum_unique_users::float-1) as pct_change
from __dbt__CTE__intm_nplus_max_cum_users_live a
left join
(select airdate, external_id, (lag(cumulative_unique_user,1) over (order by airdate asc,time_interval asc)) prev_max_cum_unique_users
 from
(select airdate, external_id, min(time_interval) as time_interval,max(cumulative_unique_user) as cumulative_unique_user
 from __dbt__CTE__intm_nplus_max_cum_users_live group by 1,2))b
on a.external_id = b.external_id and a.airdate = b.airdate
),  __dbt__CTE__intm_nplus_content_nxt_tko as (

select distinct premiere_date, network_id, episode_nm as title from "entdwdb"."cdm"."dim_content_classification_title"
        where series_group = 'NXT TakeOver' and (premiere_date >= current_date-7 and  premiere_date <=current_date) and right(episode_nm,4) != '(ES)' 
        and production_id not like '%span_ntwk%'
),  __dbt__CTE__intm_nplus_viewershipdata_with_externalid_nxt_tko as (


select  src_fan_id, stream_id, stream_start_dttm as min_time, stream_end_dttm as max_time, time_spent, external_id, stream_type_cd, 
         stream_device_platform,content_wwe_id, content_duration, live_start_schedule as live_start,
         live_end_schedule as live_end, first_stream, _same_day, _3day_dt, _7day_dt, _30day_dt       
        from "entdwdb"."fds_nplus"."fact_daily_content_viewership" 
        where external_id in (select network_id from __dbt__CTE__intm_nplus_content_nxt_tko)
        and trunc(live_start_schedule) in (select premiere_date from __dbt__CTE__intm_nplus_content_nxt_tko)
),  __dbt__CTE__intm_nplus_max_cum_users_nxt_tko as (

select *, (max(cumulative_unique_user) over (partition by external_id, premiere_date)) as max_cum_unique_users
from "entdwdb"."fds_nplus"."rpt_nplus_daily_nxt_tko_streams" where premiere_date >= current_date-7
),  __dbt__CTE__intm_nplus_cg_nxt_tko_streams_v2 as (

select a.*, (max_cum_unique_users::float/ prev_max_cum_unique_users::float-1) as pct_change
from __dbt__CTE__intm_nplus_max_cum_users_nxt_tko a
left join
(select premiere_date, external_id, (lag(cumulative_unique_user,1) over (order by premiere_date asc,time_interval asc)) prev_max_cum_unique_users
 from
(select premiere_date, external_id, min(time_interval) as time_interval,max(cumulative_unique_user) as cumulative_unique_user
 from __dbt__CTE__intm_nplus_max_cum_users_nxt_tko group by 1,2))b
on a.external_id = b.external_id and a.premiere_date = b.premiere_date
)select type,external_id,title,premiere_date,complete_rate,viewers_count,etl_batch_id,etl_insert_user_id,etl_insert_rec_dttm,etl_update_user_id,etl_update_rec_dttm
from 
(
select distinct 'PPV' as type, external_id, title, premiere_date, round(complete_rate,2):: float(2) complete_rate,
 count(src_fan_id) as viewers_count,
 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
  from
(select distinct *, 
case when show_sec <viewed_sec then 1.00 
     when show_sec = 0  then 0
else viewed_sec/show_sec end as complete_rate 
from
        (select distinct *, 
        content_duration as show_sec from
                (select distinct a.*, b.title, b.content_duration, b.premiere_date from 
                (select distinct external_id,src_fan_id,sum(case when time_spent<0 then 0 else time_spent end) as viewed_sec
                from __dbt__CTE__intm_nplus_viewership_data_with_externalid 
				where external_id in (select external_id from "entdwdb"."fds_nplus"."rpt_nplus_daily_ppv_streams")
				group by 1,2) a
                left join __dbt__CTE__intm_nplus_cg_ppv_streams_v2 b on a.external_id = b.external_id)
        )
)
group by 1,2,3,4,5 order by 1,2,3,4,5)

union all --live stream
select type,external_id,title,premiere_date,complete_rate,viewers_count,etl_batch_id,etl_insert_user_id,etl_insert_rec_dttm,etl_update_user_id,etl_update_rec_dttm
from 
(
select distinct '205 Live' as type, external_id, title, premiere_date, round(complete_rate,2):: float(2) complete_rate,
 count(src_fan_id) as viewers_count,
 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
  from
(select distinct *, 
case when show_sec <viewed_sec then 1.00 
     when show_sec = 0  then 0
else viewed_sec/cast (show_sec as int) end as complete_rate 
from
        (select distinct *, 
        content_duration as show_sec from
                (select distinct a.*, b.title, b.content_duration, b.airdate as premiere_date from 
                (select distinct external_id,src_fan_id,sum(case when time_spent<0 then 0 else time_spent end) as viewed_sec
                from __dbt__CTE__intm_nplus_viewershipdata_with_externalid_live
                where external_id in (select external_id from "entdwdb"."fds_nplus"."rpt_nplus_daily_live_streams")				
				group by 1,2) a
                left join __dbt__CTE__intm_nplus_cg_live_streams_v2 b on a.external_id = b.external_id)
        )
)
group by 1,2,3,4,5 order by 1,2,3,4,5)

union all --Nxt_tko stream
select type,external_id,title,premiere_date,complete_rate,viewers_count,etl_batch_id,etl_insert_user_id,etl_insert_rec_dttm,etl_update_user_id,etl_update_rec_dttm
from 
(
select distinct 'NXT' as type, external_id, title, premiere_date, round(complete_rate,2):: float(2) complete_rate,
 count(src_fan_id) as viewers_count,
 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
  from
(select distinct *, 
case when show_sec <viewed_sec then 1.00 
     when show_sec = 0  then 0
else viewed_sec/show_sec end as complete_rate 
from
        (select distinct *, 
        content_duration as show_sec from
                (select distinct a.*, b.title, b.content_duration, b.premiere_date from 
                (select distinct external_id,src_fan_id,sum(case when time_spent<0 then 0 else time_spent end) as viewed_sec
                from __dbt__CTE__intm_nplus_viewershipdata_with_externalid_nxt_tko 
				where external_id in (select external_id from "entdwdb"."fds_nplus"."rpt_nplus_daily_nxt_tko_streams")
				group by 1,2) a
                left join __dbt__CTE__intm_nplus_cg_nxt_tko_streams_v2 b on a.external_id = b.external_id)
        )
)
group by 1,2,3,4,5 order by 1,2,3,4,5)