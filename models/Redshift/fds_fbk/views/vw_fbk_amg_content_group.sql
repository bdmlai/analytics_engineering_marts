{{
  config({
		'schema': 'fds_fbk',
		"materialized": 'view',
		'post-hook': 'grant select on {{this}} to public'
  })
}}

select * from  {{ref('fbk_amg_content_group')}}