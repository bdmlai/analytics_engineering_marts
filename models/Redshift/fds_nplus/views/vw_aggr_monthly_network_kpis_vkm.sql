 {{
  config({
    "schemas": 'fds_nplus',
	"materialized": 'view',
	})
}}
select * from {{ref('aggr_monthly_network_kpis_vkm')}}