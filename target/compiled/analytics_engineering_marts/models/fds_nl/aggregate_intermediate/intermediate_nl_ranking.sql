



 with __dbt__CTE__intermediate_nl_minxmin_ratings as (


select  src_market_break,src_demographic_group,broadcast_date,mxm_source,
case when upper(src_broadcast_network_name)= 'FOX' then 'FOX Affiliates' 
when upper(src_broadcast_network_name) = 'TURNER NETWORK TELEVISION' then 'TNT'
WHEN  upper(src_broadcast_network_name) = 'USA NETWORK' THEN 'USA' END AS src_broadcast_network_name,
((split_part(program_telecast_rpt_starttime, ':', 1) :: int *60*60 +
 split_part(program_telecast_rpt_starttime, ':', 2) :: int*60 +
  (split_part(program_telecast_rpt_starttime, ':', 3) :: int  ))
 + (min_of_pgm_value - 1)*60) AS TIME_MINUTE ,
most_current_us_audience_avg_proj_000,etl_insert_rec_dttm
FROM  "entdwdb"."fds_nl"."fact_nl_minxmin_ratings" 
where src_playback_period_cd in ('Live | TV with Digital | Linear with VOD')
),  __dbt__CTE__intermediate_nl_switching_absolute_network_num as (


select distinct a.coverage_area,a.src_market_break,a.src_demographic_group,a.broadcast_date,a.src_broadcast_network_name,
a.switching_behavior_dist_cd,a.source_name,
a.time_minute ,b.most_current_us_audience_avg_proj_000,a.switching_behavior_dist_Cd_Value,
((switching_behavior_dist_Cd_Value/100 ) * most_current_us_audience_avg_proj_000 ) as absolute_network_number 
 from "entdwdb"."fds_nl"."fact_nl_weekly_live_switching_behavior_destination_dist"  a
join  __dbt__CTE__intermediate_nl_minxmin_ratings b
 on (a.src_market_break)= (b.src_market_break)
and (a.src_demographic_group) = (b.src_demographic_group)
 and a.broadcast_date=b.broadcast_date
and (a.src_broadcast_network_name)= (b.src_broadcast_network_name) and 
upper(a.source_name) = upper(b.mxm_source) and
(split_part(a.time_minute, ':', 1) :: int *60*60 +
 split_part(a.time_minute, ':', 2) :: int*60 +
  (split_part(a.time_minute, ':', 3) :: int)) = b.time_minute
),  __dbt__CTE__intermediate_nl_absolute_network_total_num as (


select a.broadcast_Date,a.coverage_area,a.src_market_break,a.src_broadcast_network_name,a.src_demographic_group,
a.time_minute,a.source_name,
most_current_us_audience_avg_proj_000,
sum(absolute_network_number) as absolute_network_number
from __dbt__CTE__intermediate_nl_switching_absolute_network_num  a 
group by 1,2,3,4,5,6,7,8
),  __dbt__CTE__intermediate_nl_absolute_usa_fox_tnt_stay as (


 select  a.coverage_area,a.src_market_break,a.src_demographic_group,
a.broadcast_Date,a.src_broadcast_network_name,a.time_minute,absolute_network_number,
 round((a.absolute_network_number/nullif(a.most_current_us_audience_avg_proj_000,0))*100 ,5)
 as stay_percent from __dbt__CTE__intermediate_nl_switching_absolute_network_num a
 where  (src_broadcast_network_name,switching_behavior_dist_cd)
in (('USA','usa'),('FOX Affiliates','fox_affiliates'),('TNT','tnt'))
),  __dbt__CTE__intermediate_nl_set_off_air_absolute_network_num as (



 select * from __dbt__CTE__intermediate_nl_switching_absolute_network_num
 where   switching_behavior_dist_cd in ('set_off_off_air')
),  __dbt__CTE__intm_nl_est_time_diff as (

select distinct c.showdbid, datediff(sec, (c.airdate || ' ' || substring(trim(c.min_inpoint), 1, 8))::timestamp, 
(d.air_date || ' ' || substring(trim(d.start_time_eastern), 1, 8))::timestamp) as est_time_diff 
from
(select distinct a.showdbid, a.min_inpoint, b.airdate, b.logname
from
(select showdbid, min(inpoint) as min_inpoint
from "entdwdb"."udl_nplus"."raw_lite_log"
where lower(trim(title)) in ('nxt','raw','smackdown') and 
showdbid is not null and showdbid <> 0 and inpoint is not null 
and inpoint <> ' ' and segmenttype is not null 
group by 1) a
join "entdwdb"."udl_nplus"."raw_lite_log" b
on a.showdbid = b.showdbid and a.min_inpoint = b.inpoint) c
join "entdwdb"."udl_emm"."emm_weekly_log_reference" d on c.airdate = d.air_date and
lower(trim(c.logname)) = lower(trim(d.logname))
where d.start_time_eastern is not null and d.start_time_eastern not in ('0', ' ')
),  __dbt__CTE__intm_nl_lite_log_est as (


select a.showdbid, title, subtitle, episodenumber, airdate, inpoint, outpoint, 
(dateadd(hr, 12, (dateadd(sec, b.est_time_diff, (airdate || ' ' || substring(trim(inpoint), 1, 8))::timestamp)))) as inpoint_24hr_est, 
((substring((dateadd(sec, 30, inpoint_24hr_est)), 1, 17) || '00')::timestamp) as modified_inpoint,
((substring((dateadd(sec, (((substring(duration, 1, 2))::int * 60 * 60) + ((substring(duration, 4, 2))::int * 60) 
+ ((substring(duration, 7, 2))::int) + 30), inpoint_24hr_est)), 1, 17) || '00')::timestamp) as modified_outpoint,
segmenttype, comment, matchtype, talentactions, move, finishtype, recorddate, fileid, duration, additionaltalent, announcers, 
matchtitle, venuelocation, venuename, issegmentmarker, logentrydbid, logentryguid, loggername, logname, 
masterclipid, modifieddatetime, networkassetid, sponsors, weapon, season, source_ffed_name 
FROM "entdwdb"."udl_nplus"."raw_lite_log" a 
join __dbt__CTE__intm_nl_est_time_diff  b on a.showdbid = b.showdbid
where airdate is not null and inpoint is not null and duration is not null
and inpoint <> ' ' and duration <> ' '
),  __dbt__CTE__intermediate_nl_absolute_switch_stay_detail as (


select a.coverage_area,a.src_market_break,a.src_demographic_group,
a.broadcast_Date,a.src_broadcast_network_name,a.time_minute,a.source_name,d.comment ,
a.most_current_us_audience_avg_proj_000,
a.absolute_network_number,
c.absolute_network_number as absolute_set_off_off_air,
b.absolute_network_number as absolute_stay,
 b.stay_percent , 
 (a.absolute_network_number-b.absolute_network_number-c.absolute_network_number) as absolute_switch ,
 round((((a.absolute_network_number-b.absolute_network_number)-c.absolute_network_number)/nullif(a.most_current_us_audience_avg_proj_000,0))*100,5) as switch_percent
FROM __dbt__CTE__intermediate_nl_absolute_network_total_num a  LEFT JOIN __dbt__CTE__intermediate_nl_absolute_usa_fox_tnt_stay  B
ON a.src_demographic_group = b.src_demographic_group and 
a.broadcast_Date = b.broadcast_Date and
a.src_broadcast_network_name = b.src_broadcast_network_name and
a.time_minute = b. time_minute
left join __dbt__CTE__intermediate_nl_set_off_air_absolute_network_num   c
ON a.src_demographic_group = c.src_demographic_group and 
a.broadcast_Date = c.broadcast_Date and
a.src_broadcast_network_name = c.src_broadcast_network_name and
a.time_minute = c. time_minute
left join (select distinct airdate,title,modified_inpoint,modified_outpoint,comment from __dbt__CTE__intm_nl_lite_log_est
 where  lower(comment) in ('commercial break'))  d 
 on trunc(a.broadcast_date) = d.airdate
and lower(trim(a.source_name)) = lower(trim(d.title)) 
and ( (trunc(a.broadcast_date) || ' ' || trim(a.time_minute))::timestamp)
>= d.modified_inpoint and ( (trunc(a.broadcast_date) || ' ' || trim(a.time_minute))::timestamp) < d.modified_outpoint
)select a.broadcast_Date,a.src_broadcast_network_name,a.src_demographic_group,
 a.time_minute,
 dense_rank() over(partition by src_broadcast_network_name,broadcast_Date,src_demographic_group  order by 
switch_percent desc NULLS LAST)
as switch_percent_rank
from __dbt__CTE__intermediate_nl_absolute_switch_stay_detail  a
where ((lower(a.source_name) in ('nxt','smackdown') and time_minute  between '20:05:00'
and '21:54:00') or (lower(a.source_name) in ('raw') and time_minute  between '20:05:00'
and '22:54:00')  or 
( lower(a.source_name)  in ('aew'))) and  comment is null