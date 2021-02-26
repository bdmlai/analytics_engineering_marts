{{
  config({
		"materialized": 'ephemeral'
		})
}}


select k.*,talent from (
select wed.video_id, trunc(wed.time_uploaded) as upload_date, wed.report_date_dt as view_date,
sum(views) as views, sum(wed.watch_time_minutes) as minutes_watched, sum(likes+comments+coalesce(shares,0)) as engagements
from {{source('fds_yt','rpt_yt_wwe_engagement_daily')}} wed --where date_trunc('month',report_date_dt)= date_trunc('month',current_date-28)

{% if is_incremental() %}
  where date_trunc('month',report_date_dt)= date_trunc('month',current_date-28)
{% endif %}


group by 1,2,3
) k
left join {{source('fds_yt','yt_amg_content_groups')}} acg on (k.video_id = acg.yt_id)