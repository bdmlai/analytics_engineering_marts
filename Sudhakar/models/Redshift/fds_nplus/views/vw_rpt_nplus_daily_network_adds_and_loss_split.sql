{{
  config({
		 'schema': 'fds_nplus',	
	     "materialized": 'view','tags': "rpt_nplus_daily_network_adds_and_loss_split",
	"post-hook" : 'grant select on {{this}} to public'

        })
}}

SELECT payment_status,
       country_cd,
       payment_method,
       order_type,
       date,
       add_flag,
       loss_reason,
       metric_type,
       fan_count
FROM {{ref('rpt_nplus_daily_network_adds_and_loss_split')}}