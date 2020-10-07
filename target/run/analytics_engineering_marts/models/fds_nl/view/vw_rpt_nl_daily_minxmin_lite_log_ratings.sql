

  create view "entdwdb"."fds_nl"."vw_rpt_nl_daily_minxmin_lite_log_ratings__dbt_tmp" as (
    
select broadcast_date_id, broadcast_date, src_broadcast_network_name, src_program_name, src_playback_period_cd, 
src_demographic_group, program_telecast_rpt_starttime, program_telecast_rpt_endtime, min_of_pgm_value, 
most_current_audience_avg_pct, most_current_us_audience_avg_proj_000, most_current_nw_cvg_area_avg_pct,
showdbid, title, subtitle, episodenumber, airdate, inpoint, outpoint, modified_inpoint, modified_outpoint,
segmenttype, comment, matchtype, talentactions, move, finishtype, recorddate, fileid, duration, additionaltalent, 
announcers, matchtitle, venuelocation, venuename, issegmentmarker, logentrydbid, logentryguid, loggername, logname, 
masterclipid, modifieddatetime, networkassetid, sponsors, weapon, season, source_ffed_name
from "entdwdb"."fds_nl"."rpt_nl_daily_minxmin_lite_log_ratings"
  ) ;
