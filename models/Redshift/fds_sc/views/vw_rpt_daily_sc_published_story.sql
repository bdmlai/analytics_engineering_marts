{{
  config({
	'schema': 'fds_sc',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on {{this}} to public'
	})
}}

select dim_smprovider_account_id,dim_platform_id,dim_story_id,dim_date_id,story_start,
       updated,story_view_time_secs,as_on_date 
from
(select *,row_number() over(partition by dim_story_id order by dim_date_id desc) as row

from {{source('fds_sc','fact_sc_published_story')}}) a
where row = 1