{{
  config({
	'schema': 'fds_igm',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on {{this}} to public'
	})
}}

select dim_smprovider_account_id,dim_social_account_id,dim_platform_id,dim_story_id,dim_date_id,
       number_of_story_posts,content_published_story_time_secs,as_on_date 
from

(select *,row_number() over(partition by dim_story_id order by dim_date_id desc) as row

from {{source('fds_igm','fact_ig_published_story')}}) a
where row = 1