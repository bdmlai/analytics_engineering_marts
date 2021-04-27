{{
  config({
	'schema': 'fds_tms',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
          'post-hook': 'grant select on {{ this }} to public'
       
	})
}}


select date
	,title
	,avg_appetite_score
	,stddev_appetite_score
	,country

from {{ref('rpt_tms_weekly_yougov_country_split')}}
