{{
  config({
		"materialized": 'ephemeral'
  })
}}
select *, (max(cumulative_unique_user) over (partition by external_id, premiere_date)) as max_cum_unique_users
from {{ref('rpt_nplus_daily_ppv_streams')}} where premiere_date >= current_date-7
