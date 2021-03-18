
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
),  __dbt__CTE__intm_nplus_viewership_sequence_generator as (


with digit as (
    select 0 as d union all 
    select 1 union all select 2 union all select 3 union all
    select 4 union all select 5 union all select 6 union all
    select 7 union all select 8 union all select 9        
),
seq as (
    select a.d + (10 * b.d) + (100 * c.d) + (1000 * d.d) + (10000 * e.d)+ (100000 * f.d) as num
    from digit a
        cross join
        digit b
        cross join
        digit c
        cross join
        digit d
        cross join
        digit e
        cross join
        digit f
        
    order by 1        
),

#series as 
(select num from seq where num <= datediff('m', ((select substring(getdate()-7,1,11)||'00:00:02')::TIMESTAMP ), (getdate()::TIMESTAMP))/5+1),

inter AS 
        (select
        (date_trunc('hour', getdate()) 
        + date_part('minute', getdate())::int / 5 * interval '5 min' - (num * interval '5 min')) + interval '2 sec'
		as intvl_dttm
        FROM #series 
        ),
       
       
llog AS (
        select premiere_date,external_id,title,segmenttype,content_duration, seg_num, milestone,matchtype,
		talentactions,move,finishtype,additionaltalent, venuelocation,venuename,state,   
		in_time_est :: timestamp as begindate, out_time_est :: timestamp as enddate,
         (lead(in_time_est, 1) OVER (partition by external_id order by in_time_est asc )) as nxt_seg_begindate
        from __dbt__CTE__intm_nplus_event_lite_log_data
        ),
 
 #T8 as 		
(SELECT *
, CASE WHEN begindate between intvl_dttm and intvl_dttm + interval '5 mins' then begindate
        WHEN enddate between  intvl_dttm - interval '5 mins' and intvl_dttm then enddate
        ELSE intvl_dttm
  END AS time_interval
FROM llog
CROSS JOIN
inter
WHERE 
(inter.intvl_dttm between (llog.begindate - interval '5 min') and (llog.enddate + interval '5 min'))
union all
SELECT *, intvl_dttm AS time_interval
FROM llog
CROSS JOIN
inter
WHERE (inter.intvl_dttm > llog.enddate and inter.intvl_dttm < llog.nxt_seg_begindate)),

#T9 as 
( select *, (lag(time_interval, 1) OVER (partition by external_id order by time_interval asc )) as prev_time_interval
        from #T8)
 select * from #T9
)select *,
(select count(distinct stream_id) from __dbt__CTE__intm_nplus_viewership_data_with_externalid b where a.external_id = b.external_id and b.min_time < a.time_interval
) as streams_count,
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewership_data_with_externalid c where a.external_id = c.external_id and (c.min_time < a.time_interval) 
) as cumulative_unique_user,
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewership_data_with_externalid  where a.external_id = external_id and min_time < a.time_interval 
and  min_time > a.prev_time_interval
) as users_added,
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewership_data_with_externalid  where a.external_id = external_id and max_time < a.time_interval and max_time > a.prev_time_interval  
) as users_exits,	
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewership_data_with_externalid  where a.external_id = external_id and min_time < a.time_interval and max_time > a.time_interval  
) as total_user,
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewership_data_with_externalid  where a.external_id = external_id and min_time < a.prev_time_interval and max_time > a.time_interval  
) as previous_seg_users,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm

from __dbt__CTE__intm_nplus_viewership_sequence_generator a