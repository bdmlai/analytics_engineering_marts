{{
  config({
	'schema': 'fds_nplus',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on {{this}} to public'
	})
}}
select *
from {{ref('rpt_nplus_daily_ppv_streams')}}