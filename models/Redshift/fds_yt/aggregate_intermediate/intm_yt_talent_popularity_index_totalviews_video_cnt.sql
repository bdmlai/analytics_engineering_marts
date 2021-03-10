{{
  config({
		"materialized": 'ephemeral'
  })
}}

select a.region_code, b.talent, a.granularity,round(COALESCE(b.normal_views/nullif(a.avg_norm_views,0)*100,0),3) as view_p_index_totalviews_video_cnt
from 
(select region_code, granularity,avg(normal_views) as avg_norm_views
from {{ref('intm_yt_talent_normal_totalviews_video_cnt')}} group by 1,2) as a
inner join 
{{ref('intm_yt_talent_normal_totalviews_video_cnt')}} b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, view_p_index_totalviews_video_cnt desc
