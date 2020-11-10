{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, (max(cum_unique_users) over (partition by external_id, premiere_date)) as max_cum_unique_users
from {{ref('rpt_nplus_daily_nxt_tko_streams')}} where premiere_date >= current_date-7
