{{
  config({
	'schema': 'fds_nplus',"materialized": 'view','tags': "rpt_nplus_daily_milestone_complete_rates","persist_docs": {'relation' : true, 'columns' : true},
	'post-hook': 'grant select on {{ this }} to public'
	})
}}
select type,
	   external_id,
	   title,
	   premiere_date,
	   complete_rate,
	   viewers_count
from {{ref('rpt_nplus_daily_milestone_complete_rates')}}