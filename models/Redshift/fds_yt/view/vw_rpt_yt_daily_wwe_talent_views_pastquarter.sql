{{
  config({
	'schema': 'fds_yt',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on {{this}} to public'
         
	})
}}
select * from {{ref('rpt_yt_daily_wwe_talent_views_pastquarter')}}