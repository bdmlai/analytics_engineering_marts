{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, (max(cum_unique_users) over (partition by external_id, airdate)) as max_cum_unique_users
from {{ref('rpt_nplus_daily_live_streams')}} where airdate >= current_date-7
