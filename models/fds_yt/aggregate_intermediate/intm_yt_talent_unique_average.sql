{{
  config({
		"materialized": 'ephemeral'
  })
}}

select a.region_code, b.talent, a.granularity
,round(cast(COALESCE(b.total_views/nullif(a.sum_views,0) ,0) as float),3) as normal_views

from 
(select region_code, granularity
,cast(sum(total_views) as float) as sum_views 
 
from {{ref('intm_yt_talent_views_consolidated')}} group by 1,2) as a
inner join 
(select region_code, talent, granularity
,cast(total_views as float) total_views

from {{ref('intm_yt_talent_views_consolidated')}}) b on a.region_code=b.region_code and a.granularity=b.granularity
order by 1,3 asc, normal_views desc