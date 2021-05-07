{{
  config({
	'schema': 'fds_fbk',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on {{this}} to public'
	})
}}

select dim_smprovider_account_id,dim_social_account_id,dim_platform_id,dim_video_id,dim_media_id,
       dim_content_type_id,dim_date_id,post_date,post_text,as_on_date 
 from
(select *,row_number() over(partition by dim_video_id,dim_media_id order by dim_date_id desc) as row

from {{source('fds_fbk','fact_fb_published_post')}}
) a
where row = 1