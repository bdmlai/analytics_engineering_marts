 {{
  config({
    "schemas": 'fds_nplus',
	"materialized": 'view',"post-hook" : 'grant select on {{this}} to public'
	})
}}
select * from {{ref('rpt_network_ppv_liveplusvod')}}