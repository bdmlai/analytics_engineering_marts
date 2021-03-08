
with __dbt__CTE__intm_nplus_content_205_Live as (


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
),  __dbt__CTE__intm_nplus_duration_205_Live as (


select * from __dbt__CTE__intm_nplus_content_205_Live a left join
(select distinct external_id, content_duration, live_start_schedule, live_end_schedule 
from "entdwdb"."fds_nplus"."fact_daily_content_viewership" 
where external_id in (select wwe_id from __dbt__CTE__intm_nplus_content_205_Live)
and trunc(live_start_schedule) in (select airdate from __dbt__CTE__intm_nplus_content_205_Live)
)b
on a.wwe_id = b.external_id
),  __dbt__CTE__intm_nplus_litelog_est_time_diff_Live as (


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
where title like '%205%' and issegmentmarker='TRUE' and comment is not null
        and segmenttype not in ('Signature','Match Milestone','Set Shot','Sponsor Element','Promo Graphic') 
        and airdate in ( select airdate from __dbt__CTE__intm_nplus_content_205_Live) 
 and showdbid is not null and showdbid <> 0 and inpoint is not null 
and inpoint <> ' ' and segmenttype is not null 
and episodenumber in (select episodenumber from __dbt__CTE__intm_nplus_content_205_Live)
group by 1) a
join "entdwdb"."udl_nplus"."raw_post_event_log" b
on a.showdbid = b.showdbid and a.min_inpoint = b.inpoint) c
join "entdwdb"."udl_emm"."emm_weekly_log_reference" d 
on c.airdate = d.air_date and
lower(trim(c.logname)) = lower(trim(d.logname))
where airdate in (select airdate from __dbt__CTE__intm_nplus_content_205_Live)
 and d.start_time_eastern is not null and d.start_time_eastern not in ('0', ' ')
),  __dbt__CTE__intm_nplus_lite_log_est_live as (


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
join __dbt__CTE__intm_nplus_litelog_est_time_diff_Live b on a.showdbid = b.showdbid
and a.airdate in (select airdate from __dbt__CTE__intm_nplus_content_205_Live)
where  airdate is not null and inpoint is not null and duration is not null
and inpoint <> ' ' and duration <> ' ' 
and title like '%205%' and issegmentmarker='TRUE' and comment is not null
--and title like '%205%' and issegmentmarker='TRUE' and comment is not null
and segmenttype not in ('Signature','Match Milestone','Set Shot','Sponsor Element','Promo Graphic')  
 and a.showdbid is not null and a.showdbid <> 0  and segmenttype is not null 
and episodenumber in (select episodenumber from __dbt__CTE__intm_nplus_content_205_Live)
),  __dbt__CTE__intm_nplus_event_litelog_live as (


select a.wwe_id as external_id,a.series_episode,a.content_duration,a.live_start_schedule, a.live_end_schedule,
b.* from __dbt__CTE__intm_nplus_duration_205_Live a 
left join __dbt__CTE__intm_nplus_lite_log_est_live b
on a.live_start_schedule<= b.in_time_est
and substring(trim(a.live_end_schedule), 1, 16) >= substring(trim(b.out_time_est), 1, 16)
),  __dbt__CTE__intm_nplus_viewership_sequence_generator_live as (


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
        select airdate,external_id,series_episode as title,segmenttype,content_duration, seg_num, milestone,matchtype,
		talentactions,move,finishtype,additionaltalent, venuelocation,venuename,state,   
		in_time_est :: timestamp as begindate, out_time_est :: timestamp as enddate,
         (lead(in_time_est, 1) OVER (partition by external_id order by in_time_est asc )) as nxt_seg_begindate
        from __dbt__CTE__intm_nplus_event_litelog_live
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
(select count(distinct stream_id) from __dbt__CTE__intm_nplus_viewershipdata_with_externalid_live b where a.external_id = b.external_id and b.min_time < a.time_interval
) as streams_count,
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewershipdata_with_externalid_live c where a.external_id = c.external_id and (c.min_time < a.time_interval) 
) as cumulative_unique_user,
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewershipdata_with_externalid_live  where a.external_id = external_id and min_time < a.time_interval 
and  min_time > a.prev_time_interval
) as users_added,
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewershipdata_with_externalid_live  where a.external_id = external_id and max_time < a.time_interval and max_time > a.prev_time_interval  
) as users_exits,	
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewershipdata_with_externalid_live  where a.external_id = external_id and min_time < a.time_interval and max_time > a.time_interval  
) as total_user,
(select count(distinct src_fan_id) from __dbt__CTE__intm_nplus_viewershipdata_with_externalid_live  where a.external_id = external_id and min_time < a.prev_time_interval and max_time > a.time_interval  
) as previous_seg_users,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm

from __dbt__CTE__intm_nplus_viewership_sequence_generator_live a