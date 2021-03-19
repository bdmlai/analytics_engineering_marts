

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