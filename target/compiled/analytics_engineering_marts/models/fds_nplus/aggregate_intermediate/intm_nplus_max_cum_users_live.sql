
select *, (max(cumulative_unique_user) over (partition by external_id, airdate)) as max_cum_unique_users
from "entdwdb"."fds_nplus"."rpt_nplus_daily_live_streams" where airdate >= current_date-7