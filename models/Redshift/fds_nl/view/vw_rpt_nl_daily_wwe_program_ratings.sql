
{{
  config({
	'schema': 'fds_nl',"materialized": 'view',"tags": 'rpt_nl_daily_wwe_program_ratings',
  "persist_docs": {'relation' : true, 'columns' : true},
  'post-hook': 'grant select on {{ this }} to public'
	})
}}
select * from {{ref('rpt_nl_daily_wwe_program_ratings')}}