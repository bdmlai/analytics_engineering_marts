{{
  config({
		 'schema': 'fds_nplus',	
	     "materialized": 'view','tags': "rpt_nplus_daily_network_ad_impression_ad_type", "persist_docs": {'relation' : true, 'columns' : true},
	"post-hook" : 'grant select on {{this}} to public'

        })
}}

select start_date, 
	start_time, 
	content_id, 
	content_description,
        duration, 
	end_time,
	ad_abbreviation, 
	ad_category, 
	ad_type, 
	audience_type, 
	 concurrent_plays 
from {{ref('rpt_nplus_daily_network_ad_impression_ad_type')}}