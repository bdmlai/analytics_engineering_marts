{{
  config({
		"materialized": 'ephemeral'
        })
}}


select *,
         case when playlist_flag=1 then 'Latest week' 
               when playlist_flag=2 then 'Previous week' 
               else 'Historical weeks' end as "Pl Flag",
          'All' as "Table Type"
  from 
	(
		select *,
            dense_rank() over (partition by brand order by week_uploaded desc) as playlist_flag,
             brand||' '||extract(dow from episode_air_date) as brand_dow 
		from
			(
				select a.*
				   ,case WHEN amg_content_group IN ('SmackDown') THEN 'SmackDown Live'
						 WHEN amg_content_group IN ('Raw') THEN 'RAW' 
						 WHEN amg_content_group IN ('NXT') THEN 'NXT'  
					ELSE '205 Live' END AS brand
					,case when lower(a.title) like '%: smackdown live%' then 'SmackDown LIVE: Catch all of the action LIVE on Friday nights!'
					      when lower(a.title) like '%: smackdown,%' then 'SmackDown LIVE: Catch all of the action LIVE on Friday nights!' 
						  when lower(a.title) like '%: smackdown 1000%' then 'SmackDown LIVE: Catch all of the action LIVE on Friday nights!' 
						  when lower(a.title) like '%: raw,%' then 'Monday Night Raw: TV' 
						  when lower(a.title) like '%: raw reunion,%' then 'Monday Night Raw: TV' 
						  when lower(a.title) like '%: wwe nxt,%' then 'WWE NXT on WWE Network' 
						else 'WWE 205 Live: High-flying Cruiserweight action weekly on Tuesdays 10E/7P ONLY on WWE Network!' 
					 end as playlist_title
					 ,trunc(a.time_uploaded)+1 as day_uploaded,
					 trunc(a.time_uploaded) as episode_air_date,
					 dense_rank() over (partition by a.video_id order by a.video_id,a.report_date_dt ) as day_flag,
					 case when day_flag=1 then 'Debut' when day_flag=2 then 'Debut+2 Days' 
						  when day_flag=3 then 'Debut+3 Days' when day_flag=4 then 'Debut+4 Days'
						  when day_flag=5 then 'Debut+5 Days'  when day_flag=6 then 'Debut+6 Days'
						  else 'Debut+7 Days' 
					 end as "Date Flag",
					 date_trunc('week',a.time_uploaded) as week_uploaded
 
			from   {{ref('intm_yt_daily_episodes_bybrand_engagement_data_consolidated')}} a
			inner join {{source('fds_yt','yt_amg_content_groups')}} b
         on a.video_id = b.yt_id
		 where (  b.amg_content_group 
				   in ('Raw' , 'NXT','SmackDown') 
				     or b.series_name in ('205 Live')
			   ) 
			and  b.amg_content_group not in ('Original')
			and  b.channel_name='WWE'
			)
    where day_flag < 8
	  and   (
				 (
					brand||' '||extract(dow from (trunc(time_uploaded))) 
						in ('RAW 1','RAW 2','SmackDown Live 5',
							'SmackDown Live 6',
							'NXT 3','NXT 4','205 Live 3',
							'205 Live 4','205 Live 5','205 Live 6'
							) 
				 )
                or
				(
					brand||' '||extract(dow from (trunc(time_uploaded))) 
						in ('RAW 1','RAW 2','SmackDown Live 2',
							'SmackDown Live 3',
							'NXT 3','NXT 4','205 Live 3',
							'205 Live 4','205 Live 5','205 Live 6'
							)
				)
			)
                    
  ) 
 