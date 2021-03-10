{{
  config({
		"materialized": 'ephemeral'
  })
}}

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
FROM {{source('udl_nplus','raw_post_event_log')}} a 
join {{ref('intm_nplus_litelog_est_time_diff')}} b on a.showdbid = b.showdbid
where  airdate is not null and inpoint is not null and duration is not null
and inpoint <> ' ' and duration <> ' ' 
and lower(title) not like '%%kickoff%%' and lower(title) not like '%%nxt%%' and lower(title) not like '%%raw%%'
        and lower(title) not like '%%smackdown%%' and lower(title) not like '%%205 live%%' and lower(title) not like '%%king%%'
        and lower(title) not like '%%tribute%%' and lower(title) not like '%%mixed match%%' and issegmentmarker='TRUE'
        and upper(segmenttype) in ('MATCH','BACKSTAGE ON CAMERA','IN-ARENA NON-MATCH','ON CAMERA','TALK SHOW')