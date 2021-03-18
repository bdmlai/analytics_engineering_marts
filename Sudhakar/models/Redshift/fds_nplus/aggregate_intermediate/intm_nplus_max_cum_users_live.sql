{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, (max(cumulative_unique_user) over (partition by external_id, airdate)) as max_cum_unique_users
from {{ref('rpt_nplus_daily_live_streams')}} where airdate >= current_date-7
