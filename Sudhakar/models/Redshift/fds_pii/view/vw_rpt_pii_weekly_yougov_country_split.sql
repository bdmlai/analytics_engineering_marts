{{
  config({
	'schema': 'fds_pii',"materialized": 'view','tags': "Content","persist_docs": {'relation' : true, 'columns' : true},
          'post-hook': 'grant select on fds_pii.vw_rpt_pii_weekly_yougov_country_split to public'
       
	})
}}


select date,title,avg_appetite_score,stddev_appetite_score,country

from {{ref('rpt_pii_weekly_yougov_country_split')}}
