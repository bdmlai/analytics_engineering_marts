{{
  config({
	'schema': 'fds_yt',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true}
	})
}}


select   a.region_code, a.talent, a.granularity, a.total_views,a.cnt_video_id as video_count,a.views_30days,
round(d.view_p_index) as popularity_index,c.view_u_index as unique_index
from {{ref('intm_yt_talent_views_consolidated')}} a
inner join {{ref('intm_yt_talent_normal_views')}} b on a.talent=b.talent and a.region_code=b.region_code and a.granularity=b.granularity
inner join {{ref('intm_yt_talent_unique_index')}} c on a.talent=c.talent and a.region_code=c.region_code and a.granularity=c.granularity
inner join {{ref('intm_yt_talent_popularity_index')}} d on a.talent=d.talent and a.region_code=d.region_code and a.granularity=d.granularity
order by 1,3 asc