
with __dbt__CTE__intm_nl_est_time_diff as (

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
)select broadcast_date_id, broadcast_date, src_broadcast_network_name, src_program_name, 
src_market_break, src_daypart_name, src_playback_period_cd, src_demographic_group, mxm_source, 
program_telecast_rpt_starttime, program_telecast_rpt_endtime, min_of_pgm_value, 
most_current_audience_avg_pct, most_current_us_audience_avg_proj_000, most_current_nw_cvg_area_avg_pct, b.*,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from "entdwdb"."fds_nl"."fact_nl_minxmin_ratings" a
join __dbt__CTE__intm_nl_lite_log_est b on trunc(a.broadcast_date) = b.airdate
and lower(trim(a.mxm_source)) = lower(trim(b.title)) and 
(dateadd(min, (a.min_of_pgm_value - 1), (trunc(a.broadcast_date) || ' ' || trim(a.program_telecast_rpt_starttime))::timestamp))
>= b.modified_inpoint and 
(dateadd(min, (a.min_of_pgm_value - 1), (trunc(a.broadcast_date) || ' ' || trim(a.program_telecast_rpt_starttime))::timestamp)) 
< b.modified_outpoint 
where a.min_of_pgm_value is not null and a.program_telecast_rpt_starttime is not null 
and a.program_telecast_rpt_starttime <> ' '