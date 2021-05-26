{{
  config({
        'schema': 'fds_nplus',
		    "materialized": 'view',"tags": 'Phase0',"persist_docs": {'relation' : true, 'columns' : true},
            "post-hook" : 'grant select on {{this}} to public'
  })
}}

SELECT  order_date,
        order_type,
        add_type,
        region,
        country,
        currency,
        adds,
        trial_converts
FROM    {{ref('rpt_nplus_daily_trial_converts')}}