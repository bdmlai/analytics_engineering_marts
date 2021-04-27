{{
  config({
		"materialized": 'ephemeral'
  })
}}
select a.* , b.account_name as channel_name
from  {{ref('intm_fbk_amg_cnt_grp_1')}} as a 
inner join 
	cdm.dim_smprovider_account as b 
on a.dim_smprovider_account_id  = b.dim_smprovider_account_id