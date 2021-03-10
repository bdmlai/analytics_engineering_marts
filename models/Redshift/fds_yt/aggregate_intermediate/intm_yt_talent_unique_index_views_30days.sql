{{
  config({
		"materialized": 'ephemeral'
  })
}}

select b.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.avg_views,0) ,0) as float),3) as view_u_index_30days

from 
(select talent, granularity,cast(avg(normal_views) as float) as avg_views 
 
from {{ref('intm_yt_talent_unique_average_views_30days')}} group by 1,2) as a
inner join 
(select region_code, talent, granularity,cast(normal_views as float) total_views

from {{ref('intm_yt_talent_unique_average_views_30days')}}) b on a.talent=b.talent and a.granularity=b.granularity
order by 1,3 asc, view_u_index_30days desc