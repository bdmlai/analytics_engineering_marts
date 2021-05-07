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
join {{ref('intm_nplus_litelog_est_time_diff_Live')}} b on a.showdbid = b.showdbid
and a.airdate in (select airdate from {{ref('intm_nplus_content_205_Live')}})
where  airdate is not null and inpoint is not null and duration is not null
and inpoint <> ' ' and duration <> ' ' 
and title like '%205%' and issegmentmarker='TRUE' and comment is not null
--and title like '%205%' and issegmentmarker='TRUE' and comment is not null
and segmenttype not in ('Signature','Match Milestone','Set Shot','Sponsor Element','Promo Graphic')  
 and a.showdbid is not null and a.showdbid <> 0  and segmenttype is not null 
and episodenumber in (select episodenumber from {{ref('intm_nplus_content_205_Live')}})
