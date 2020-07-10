
{{
  config({
		"materialized": 'ephemeral'
  })
}}


 select * from {{ref('intermediate_nl_switching_absolute_network_num')}}
 where   switching_behavior_dist_cd in ('set_off_off_air')
