{{
  config({
		'schema': 'fds_fbk',
		"materialized": 'view',
		'post-hook': 'grant select on fds_fbk.vw_rpt_fbk_amg_content_group to public'
  })
}}

select * from  {{ref('rpt_fbk_amg_content_group')}}