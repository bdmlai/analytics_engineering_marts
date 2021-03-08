

select a.region_code, b.talent, a.granularity,round(COALESCE(b.normal_views/nullif(a.avg_norm_views,0)*100,0),3) as view_p_index_30days
from 
(select region_code, granularity,avg(normal_views) as avg_norm_views
from __dbt__CTE__intm_yt_talent_normal_views_30days group by 1,2) as a
inner join 
__dbt__CTE__intm_yt_talent_normal_views_30days b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, view_p_index_30days desc