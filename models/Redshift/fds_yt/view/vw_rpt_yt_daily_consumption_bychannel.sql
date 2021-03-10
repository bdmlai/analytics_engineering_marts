{{
  config({
		 'schema': 'fds_yt',	
	     "materialized": 'view',"persist_docs": {'relation' : true, 'columns' : true}
        })
}}

select * from {{ref('rpt_yt_daily_consumption_bychannel')}}


