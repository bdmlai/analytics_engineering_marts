{{
  config({
		'schema': 'fds_nl',
		"pre-hook": ["truncate fds_nl.rpt_nl_daily_minxmin_lite_log_ratings"],
		"materialized": 'incremental','tags': "Phase4B"
  })
}}
select broadcast_date_id, broadcast_date, src_broadcast_network_name, src_program_name, 
src_market_break, src_daypart_name, src_playback_period_cd, src_demographic_group, mxm_source, 
program_telecast_rpt_starttime, program_telecast_rpt_endtime, min_of_pgm_value, 
most_current_audience_avg_pct, most_current_us_audience_avg_proj_000, most_current_nw_cvg_area_avg_pct, 
b.showdbid, b.title, b.subtitle, b.episodenumber, b.airdate, b.inpoint, b.outpoint, 
 b.inpoint_24hr_est,b.modified_inpoint_1 as modified_inpoint,b.modified_outpoint_1 as modified_outpoint,
b.segmenttype, b.comment, b.matchtype, b.talentactions, b.move, b.finishtype, b.recorddate, b.fileid, b.duration, 
b.additionaltalent, b.announcers, b.matchtitle, b.venuelocation, b.venuename, b.issegmentmarker, b.logentrydbid,
 b.logentryguid, b.loggername, b.logname, b.masterclipid, b.modifieddatetime, b.networkassetid, b.sponsors, b.weapon,
 b.season, b.source_ffed_name,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_4B' as etl_batch_id, 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from {{source('fds_nl','fact_nl_minxmin_ratings')}} a
join {{ref('intm_nl_lite_log_est_modified')}} b on trunc(a.broadcast_date) = b.airdate
and lower(trim(a.mxm_source)) = lower(trim(b.title)) and 
(dateadd(min, (a.min_of_pgm_value - 1), (trunc(a.broadcast_date) || ' ' || trim(a.program_telecast_rpt_starttime))::timestamp))
>= b.modified_inpoint and 
(dateadd(min, (a.min_of_pgm_value - 1), (trunc(a.broadcast_date) || ' ' || trim(a.program_telecast_rpt_starttime))::timestamp)) 
< b.modified_outpoint 
where a.min_of_pgm_value is not null and a.program_telecast_rpt_starttime is not null 
and a.program_telecast_rpt_starttime <> ' '