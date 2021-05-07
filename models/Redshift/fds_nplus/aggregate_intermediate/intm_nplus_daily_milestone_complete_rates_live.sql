{{
  config({
		"materialized": 'ephemeral'
  })
}}

select type,external_id,
       title,premiere_date,
	   complete_rate,viewers_count
	    
from
(
 select distinct '205 Live' as type,  external_id, series_episode as title, 
 airdate::varchar as premiere_date, 
 round(complete_rate,2)  complete_rate, count(src_fan_id) as viewers_count from
(select distinct *, case when viewed_mins>content_duration then 1.00 else viewed_mins/content_duration end as complete_rate from
                (select distinct a.*, b.series_episode,
                b.airdate from 
                (select distinct external_id, src_fan_id, content_duration/60 as content_duration ,
                sum((case when time_spent<0 then 0 else time_spent end)) as viewed_mins
                from (select * from {{ref('intm_nplus_viewershipdata_with_externalid_live')}} 
                where  min_time between live_start and live_end) group by 1,2,3) a
                left join {{ref('intm_nplus_event_litelog_live')}} b 
                on a.external_id = b.external_id
                )
)
group by 1,2,3,4,5 order by 1,2,3,4,5)
