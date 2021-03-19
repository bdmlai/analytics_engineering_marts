


select * from "entdwdb"."fds_yt"."rpt_yt_daily_wwe_talent_views_pastquarter"
union all
select * from "entdwdb"."fds_yt"."rpt_yt_daily_wwe_talent_views_halfyear"
union all
select * from "entdwdb"."fds_yt"."rpt_yt_daily_wwe_talent_views_pastyear"
union all
select * from "entdwdb"."fds_yt"."rpt_yt_daily_wwe_talent_views_since_upload"