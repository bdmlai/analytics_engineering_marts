{{
  config({
	'schema': 'fds_nplus',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true}
	})
}}
select *
from {{ref('rpt_nplus_daily_milestone_complete_rates')}}