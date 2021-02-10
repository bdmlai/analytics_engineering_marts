{{
  config({
		"materialized": 'ephemeral'
  })
}}
select * from {{ref('rpt_le_daily_kff_state_regulation')}} 
where state_regulation_update_date >= (select max(state_regulation_update_date) from {{ref('rpt_le_daily_kff_state_regulation')}} ) - 7