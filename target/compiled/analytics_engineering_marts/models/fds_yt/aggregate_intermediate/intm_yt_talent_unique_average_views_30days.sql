

select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0) as float),3) as normal_views

from 
(select region_code, granularity,cast(sum(views_30days) as float) as sum_views 
 from __dbt__CTE__intm_yt_talent_views_consolidated group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast(views_30days as float) total_views
from __dbt__CTE__intm_yt_talent_views_consolidated) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc