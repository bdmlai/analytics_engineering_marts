{{
  config({
	'schema': 'fds_yt',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true}
         
	})
}}
select * from {{ref('rpt_yt_daily_wwe_talent_views_pastyear')}}