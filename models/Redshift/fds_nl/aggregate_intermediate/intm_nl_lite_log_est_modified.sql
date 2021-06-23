{{
  config({
		"materialized": 'ephemeral'
  })
}}

select a.showdbid, a.title, a.subtitle, a.episodenumber, a.airdate, a.inpoint, a.outpoint, 
 a.inpoint_24hr_est, a.modified_inpoint,a.modified_outpoint,
 
 abs(datediff(sec, dateadd(hr,12,(a.airdate || ' ' || substring(trim(a.inpoint), 1, 8))::timestamp), a.modified_inpoint)) as in_diff,
 abs(datediff(sec, dateadd(hr,12,(a.airdate || ' ' || substring(trim(a.outpoint), 1, 8))::timestamp), a.modified_outpoint)) as out_diff,
 
 case when a.modified_inpoint=a.modified_outpoint and in_diff < out_diff 
 then dateadd(min,1,a.modified_outpoint)
 else a.modified_outpoint end as modified_outpoint_1,
 
 case when a.modified_inpoint=a.modified_outpoint and in_diff > out_diff 
 then dateadd(min,-1,a.modified_inpoint)
 else a.modified_inpoint end as modified_inpoint_1,
 
a.segmenttype, a.comment, a.matchtype, a.talentactions, a.move, a.finishtype, a.recorddate, a.fileid,a.duration, a.additionaltalent, a.announcers, 
a.matchtitle, a.venuelocation, a.venuename, a.issegmentmarker, a.logentrydbid, a.logentryguid, a.loggername, a.logname, 
a.masterclipid, a.modifieddatetime, a.networkassetid, a.sponsors, a.weapon, a.season, a.source_ffed_name 
FROM {{ref('intm_nl_lite_log_est')}}  a 