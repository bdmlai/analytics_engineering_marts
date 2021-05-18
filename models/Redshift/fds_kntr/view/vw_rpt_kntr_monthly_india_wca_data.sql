{{
  config({
        'schema': 'fds_kntr',
		"materialized": 'view',"tags": 'Phase4B',"persist_docs": {'relation' : true, 'columns' : true},
            "post-hook" : 'grant select on {{this}} to public'
  })
}}

SELECT * FROM {{ref('rpt_kntr_monthly_india_wca_data')}}


