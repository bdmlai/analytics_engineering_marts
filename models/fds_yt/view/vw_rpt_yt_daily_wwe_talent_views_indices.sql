{{
  config({
	'schema': 'fds_yt',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
        'post-hook': 'grant select on fds_yt.vw_rpt_yt_daily_wwe_talent_views_indices to public'
         
	})
}}


select  a.region_code, a.talent, a.granularity, a.total_views,a.cnt_video_id as video_count,a.views_30days,
round(d.view_p_index) as popularity_index,c.view_u_index as unique_index,
round(g.view_p_index_30days) as popularity_index_30days,f.view_u_index_30days as unique_index_30days,
round(j.view_p_index_totalviews_video_cnt) as popularity_index_totalviews_per_video,i.view_u_index_totalviews_per_video as unique_index_totalviews_per_video,
round(m.view_p_index_views_30days_video_cnt) as popularity_index_views_30days_per_video,l.view_u_index_views_30days_per_video as unique_index_views_30days_per_video
from  {{ref('intm_yt_talent_views_consolidated')}} a
inner join {{ref('intm_yt_talent_normal_views')}} b on a.talent=b.talent and a.region_code=b.region_code and a.granularity=b.granularity
inner join {{ref('intm_yt_talent_unique_index')}} c on b.talent=c.talent and b.region_code=c.region_code and b.granularity=c.granularity
inner join {{ref('intm_yt_talent_popularity_index')}} d on c.talent=d.talent and c.region_code=d.region_code and c.granularity=d.granularity
inner join {{ref('intm_yt_talent_normal_views_30days')}} e on a.talent=e.talent and a.region_code=e.region_code and a.granularity=e.granularity
inner join {{ref('intm_yt_talent_unique_index_views_30days')}} f on e.talent=f.talent and e.region_code=f.region_code and e.granularity=f.granularity
inner join {{ref('intm_yt_talent_popularity_index_views_30days')}} g on f.talent=g.talent and f.region_code=g.region_code and f.granularity=g.granularity
inner join {{ref('intm_yt_talent_normal_totalviews_video_cnt')}} h on a.talent=h.talent and a.region_code=h.region_code and a.granularity=h.granularity
inner join {{ref('intm_yt_talent_unique_index_totalviews_video_cnt')}} i on h.talent=i.talent and h.region_code=i.region_code and h.granularity=i.granularity
inner join {{ref('intm_yt_talent_popularity_index_totalviews_video_cnt')}} j on i.talent=j.talent and i.region_code=j.region_code and i.granularity=j.granularity
inner join {{ref('intm_yt_talent_normal_views_30days_video_cnt')}} k on a.talent=k.talent and a.region_code=k.region_code and a.granularity=k.granularity
inner join {{ref('intm_yt_talent_unique_index_views_30days_video_cnt')}} l on k.talent=l.talent and k.region_code=l.region_code and k.granularity=l.granularity
inner join {{ref('intm_yt_talent_popularity_index_views_30days_video_cnt')}} m on l.talent=m.talent and l.region_code=m.region_code and l.granularity=m.granularity
order by 1,3 asc