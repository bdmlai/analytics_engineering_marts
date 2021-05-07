 {{
  config({
    "schemas": 'fds_nplus',
	"materialized": 'view',"post-hook" : 'grant select on {{this}} to public'
	})
}}
select * from {{ref('aggr_monthly_network_kpis_vkm')}}