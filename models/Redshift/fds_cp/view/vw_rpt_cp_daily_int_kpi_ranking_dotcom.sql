{{
  config({
		 'schema': 'fds_cp',	
	     "materialized": 'view',"persist_docs": {'relation' : true, 'columns' : true}
        })
}}

select * from {{ref('rpt_cp_daily_int_kpi_ranking_dotcom')}}


