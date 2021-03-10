{{
  config({
	'schema': 'fds_yt',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
        'post-hook': 'grant select on fds_yt.vw_rpt_yt_daily_wwe_talent_views_indices to public'
         
	})
}}



with __dbt__CTE__intm_yt_talent_views_consolidated as (


select * from {{ref('vw_rpt_yt_daily_wwe_talent_views_pastquarter')}}
union all
select * from {{ref('vw_rpt_yt_daily_wwe_talent_views_halfyear')}}
union all
select * from {{ref('vw_rpt_yt_daily_wwe_talent_views_pastyear')}}
union all
select * from {{ref('vw_rpt_yt_daily_wwe_talent_views_since_upload')}}

),  __dbt__CTE__intm_yt_talent_normal_views as (


select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0)*100 as float),3) as normal_views

from 
(select region_code, granularity
,cast(sum(total_views) as float) as sum_views 
 
from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity
,cast(total_views as float) total_views

from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc
),  __dbt__CTE__intm_yt_talent_unique_average as (


select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0) as float),3) as normal_views

from 
(select region_code, granularity
,cast(sum(total_views) as float) as sum_views 
 
from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity
,cast(total_views as float) total_views

from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc
),  __dbt__CTE__intm_yt_talent_unique_index as (

select b.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.avg_views,0) ,0) as float),3) as view_u_index

from 
(select talent, granularity
,cast(avg(normal_views) as float) as avg_views 
 
from __dbt__CTE__intm_yt_talent_unique_average group by 1,2) as a
inner join 
(select region_code, talent, granularity
,cast(normal_views as float) total_views

from __dbt__CTE__intm_yt_talent_unique_average) b on a.talent=b.talent and a.granularity=b.granularity
order by 1,3 asc, view_u_index desc
),  __dbt__CTE__intm_yt_talent_popularity_index as (


select a.region_code, b.talent, a.granularity
,round(COALESCE(b.normal_views/nullif(a.avg_norm_views,0)*100,0),3) as view_p_index

from 
(select region_code, granularity,avg(normal_views) as avg_norm_views
from __dbt__CTE__intm_yt_talent_normal_views group by 1,2) as a
inner join 
__dbt__CTE__intm_yt_talent_normal_views b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, view_p_index desc
),  __dbt__CTE__intm_yt_talent_normal_views_30days as (


select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0)*100 as float),3) as normal_views

from 
(select region_code, granularity,cast(sum(views_30days) as float) as sum_views 
from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast(views_30days as float) total_views
from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc
),  __dbt__CTE__intm_yt_talent_unique_average_views_30days as (


select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0) as float),3) as normal_views

from 
(select region_code, granularity,cast(sum(views_30days) as float) as sum_views 
 from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast(views_30days as float) total_views
from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc
),  __dbt__CTE__intm_yt_talent_unique_index_views_30days as (


select b.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.avg_views,0) ,0) as float),3) as view_u_index_30days

from 
(select talent, granularity,cast(avg(normal_views) as float) as avg_views 
 
from __dbt__CTE__intm_yt_talent_unique_average_views_30days group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast(normal_views as float) total_views

from __dbt__CTE__intm_yt_talent_unique_average_views_30days) b on a.talent=b.talent and a.granularity=b.granularity
order by 1,3 asc, view_u_index_30days desc
),  __dbt__CTE__intm_yt_talent_popularity_index_views_30days as (


select a.region_code, b.talent, a.granularity,round(COALESCE(b.normal_views/nullif(a.avg_norm_views,0)*100,0),3) as view_p_index_30days
from 
(select region_code, granularity,avg(normal_views) as avg_norm_views
from __dbt__CTE__intm_yt_talent_normal_views_30days group by 1,2) as a
inner join 
__dbt__CTE__intm_yt_talent_normal_views_30days b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, view_p_index_30days desc
),  __dbt__CTE__intm_yt_talent_normal_totalviews_video_cnt as (


select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0)*100 as float),3) as normal_views

from 
(select region_code, granularity,cast(sum(total_views)/sum(cnt_video_id) as float) as sum_views 
from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast((total_views/cnt_video_id) as float) total_views
from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc
),  __dbt__CTE__intm_yt_talent_unique_average_totalviews_video_cnt as (


select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0) as float),3) as normal_views

from 
(select region_code, granularity,cast(sum(total_views)/sum(cnt_video_id) as float) as sum_views 
 from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast((total_views/cnt_video_id) as float) total_views
from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc
),  __dbt__CTE__intm_yt_talent_unique_index_totalviews_video_cnt as (

select b.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.avg_views,0) ,0) as float),3) as view_u_index_totalviews_per_video

from 
(select talent, granularity
,cast(avg(normal_views) as float) as avg_views 
 
from __dbt__CTE__intm_yt_talent_unique_average_totalviews_video_cnt group by 1,2) as a
inner join 
(select region_code, talent, granularity
,cast(normal_views as float) total_views

from __dbt__CTE__intm_yt_talent_unique_average_totalviews_video_cnt) b on a.talent=b.talent and a.granularity=b.granularity
order by 1,3 asc, view_u_index_totalviews_per_video desc
),  __dbt__CTE__intm_yt_talent_popularity_index_totalviews_video_cnt as (


select a.region_code, b.talent, a.granularity,round(COALESCE(b.normal_views/nullif(a.avg_norm_views,0)*100,0),3) as view_p_index_totalviews_video_cnt
from 
(select region_code, granularity,avg(normal_views) as avg_norm_views
from __dbt__CTE__intm_yt_talent_normal_totalviews_video_cnt group by 1,2) as a
inner join 
__dbt__CTE__intm_yt_talent_normal_totalviews_video_cnt b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, view_p_index_totalviews_video_cnt desc
),  __dbt__CTE__intm_yt_talent_normal_views_30days_video_cnt as (

select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0)*100 as float),3) as normal_views

from 
(select region_code, granularity,cast(sum(views_30days)/sum(cnt_video_id) as float) as sum_views 
from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast((views_30days/cnt_video_id) as float) total_views
from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc
),  __dbt__CTE__intm_yt_talent_unique_average_views_30days_video_cnt as (

select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0) as float),3) as normal_views

from 
(select region_code, granularity,cast(sum(views_30days)/sum(cnt_video_id) as float) as sum_views 
 from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast((views_30days/cnt_video_id) as float) total_views
from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc
),  __dbt__CTE__intm_yt_talent_unique_index_views_30days_video_cnt as (


select b.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.avg_views,0) ,0) as float),3) as view_u_index_views_30days_per_video

from 
(select talent, granularity
,cast(avg(normal_views) as float) as avg_views 
 
from __dbt__CTE__intm_yt_talent_unique_average_views_30days_video_cnt group by 1,2) as a
inner join 
(select region_code, talent, granularity
,cast(normal_views as float) total_views

from __dbt__CTE__intm_yt_talent_unique_average_views_30days_video_cnt) b on a.talent=b.talent and a.granularity=b.granularity
order by 1,3 asc, view_u_index_views_30days_per_video desc
),  __dbt__CTE__intm_yt_talent_popularity_index_views_30days_video_cnt as (


select a.region_code, b.talent, a.granularity,round(COALESCE(b.normal_views/nullif(a.avg_norm_views,0)*100,0),3) as view_p_index_views_30days_video_cnt
from 
(select region_code, granularity,avg(normal_views) as avg_norm_views
from __dbt__CTE__intm_yt_talent_normal_views_30days_video_cnt group by 1,2) as a
inner join 
__dbt__CTE__intm_yt_talent_normal_views_30days_video_cnt b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, view_p_index_views_30days_video_cnt desc
)select  a.region_code,a.region_name, a.talent, a.granularity, a.total_views,a.cnt_video_id as video_count,a.views_30days,
round(d.view_p_index) as popularity_index,c.view_u_index as unique_index,
round(g.view_p_index_30days) as popularity_index_30days,f.view_u_index_30days as unique_index_30days,
round(j.view_p_index_totalviews_video_cnt) as popularity_index_totalviews_per_video,i.view_u_index_totalviews_per_video as unique_index_totalviews_per_video,
round(m.view_p_index_views_30days_video_cnt) as popularity_index_views_30days_per_video,l.view_u_index_views_30days_per_video as unique_index_views_30days_per_video
from  __dbt__CTE__intm_yt_talent_views_consolidated a
inner join __dbt__CTE__intm_yt_talent_normal_views b on a.talent=b.talent and a.region_code=b.region_code and a.granularity=b.granularity
inner join __dbt__CTE__intm_yt_talent_unique_index c on b.talent=c.talent and b.region_code=c.region_code and b.granularity=c.granularity
inner join __dbt__CTE__intm_yt_talent_popularity_index d on c.talent=d.talent and c.region_code=d.region_code and c.granularity=d.granularity
inner join __dbt__CTE__intm_yt_talent_normal_views_30days e on a.talent=e.talent and a.region_code=e.region_code and a.granularity=e.granularity
inner join __dbt__CTE__intm_yt_talent_unique_index_views_30days f on e.talent=f.talent and e.region_code=f.region_code and e.granularity=f.granularity
inner join __dbt__CTE__intm_yt_talent_popularity_index_views_30days g on f.talent=g.talent and f.region_code=g.region_code and f.granularity=g.granularity
inner join __dbt__CTE__intm_yt_talent_normal_totalviews_video_cnt h on a.talent=h.talent and a.region_code=h.region_code and a.granularity=h.granularity
inner join __dbt__CTE__intm_yt_talent_unique_index_totalviews_video_cnt i on h.talent=i.talent and h.region_code=i.region_code and h.granularity=i.granularity
inner join __dbt__CTE__intm_yt_talent_popularity_index_totalviews_video_cnt j on i.talent=j.talent and i.region_code=j.region_code and i.granularity=j.granularity
inner join __dbt__CTE__intm_yt_talent_normal_views_30days_video_cnt k on a.talent=k.talent and a.region_code=k.region_code and a.granularity=k.granularity
inner join __dbt__CTE__intm_yt_talent_unique_index_views_30days_video_cnt l on k.talent=l.talent and k.region_code=l.region_code and k.granularity=l.granularity
inner join __dbt__CTE__intm_yt_talent_popularity_index_views_30days_video_cnt m on l.talent=m.talent and l.region_code=m.region_code and l.granularity=m.granularity
order by 1,3 asc