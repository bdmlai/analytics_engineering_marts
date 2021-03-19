
select trim(name) as talent,
		date_trunc('month', view_date) as month, 
		'YouTube' as platform,
		'Video Views' as Metric,
		sum(tot_views) as Value
from (select a.video_id,a.view_date,a.views as tot_views,a.minutes_watched as tot_min_wat,
a.engagements as tot_eng, b.name from __dbt__CTE__wwe_engagement_videos a
left join (select video_id, name from __dbt__CTE__wwe_engagement_videos_split) b 
on a.video_id=b.video_id)
group by 1,2