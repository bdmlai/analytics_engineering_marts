

select
yt_daily_revenue.channel_id,
dc.channel_name,
yt_daily_revenue.video_id,
dv.title,
cast(to_char(yt_daily_revenue.report_date,'yyyymmdd') as INT) as report_date,
yt_daily_revenue.country_code,
dco.country_name,
dco.region,
dco.ad_reporting_breakdown as ad_region,
yt_daily_revenue.uploader_type,
yt_daily_revenue.views,
yt_daily_revenue.watch_time_minutes,
yt_daily_revenue.red_views,
yt_daily_revenue.red_watch_time_minutes,
yt_daily_revenue.estimated_partner_revenue,
yt_daily_revenue.estimated_partner_ad_revenue,
yt_daily_revenue.estimated_partner_ad_auction_revenue,
yt_daily_revenue.estimated_partner_ad_reserved_revenue,
yt_daily_revenue.estimated_youtube_ad_revenue,
yt_daily_revenue.estimated_monetized_playbacks,
yt_daily_revenue.ad_impressions,
yt_daily_revenue.estimated_partner_red_revenue,
yt_daily_revenue.estimated_partner_transaction_revenue,
yt_daily_revenue.ad_type,
yt_daily_revenue.ad_Description,
yt_daily_revenue.gross_revenue,
yt_daily_revenue.gross_add_impressions,

getDate() as insert_timestamp
INTO #Temp
from
(
select
case when REV_CONSUMPTION.channel_id is not null then REV_CONSUMPTION.channel_id
when daily_ad_revenue.channel_id is not null then daily_ad_revenue.channel_id
when daily_estimated_revenue.channel_id is not null then daily_estimated_revenue.channel_id
else null end as channel_id,

case when REV_CONSUMPTION.video_id is not null then REV_CONSUMPTION.video_id
when daily_ad_revenue.video_id is not null then daily_ad_revenue.video_id
when daily_estimated_revenue.video_id is not null then daily_estimated_revenue.video_id
else null end as video_id,


case when REV_CONSUMPTION.country_code is not null then REV_CONSUMPTION.country_code
when daily_ad_revenue.country_code is not null then daily_ad_revenue.country_code
when daily_estimated_revenue.country_code is not null then daily_estimated_revenue.country_code
else null end as country_code,


case when REV_CONSUMPTION.uploader_type is not null then REV_CONSUMPTION.uploader_type
when daily_ad_revenue.uploader_type is not null then daily_ad_revenue.uploader_type
when daily_estimated_revenue.uploader_type is not null then daily_estimated_revenue.uploader_type
else null end as uploader_type,

case when REV_CONSUMPTION.report_date is not null then REV_CONSUMPTION.report_date
when daily_ad_revenue.report_date is not null then daily_ad_revenue.report_date
when daily_estimated_revenue.report_date is not null then daily_estimated_revenue.report_date
else null end as report_date,
REV_CONSUMPTION.views,
REV_CONSUMPTION.watch_time_minutes,
REV_CONSUMPTION.red_views,
REV_CONSUMPTION.red_watch_time_minutes,
daily_estimated_revenue.estimated_partner_revenue,
daily_estimated_revenue.estimated_partner_ad_revenue,
daily_estimated_revenue.estimated_partner_ad_auction_revenue,
daily_estimated_revenue.estimated_partner_ad_reserved_revenue,
daily_estimated_revenue.estimated_youtube_ad_revenue,
daily_estimated_revenue.estimated_monetized_playbacks,
daily_estimated_revenue.ad_impressions,
daily_estimated_revenue.estimated_partner_red_revenue,
daily_estimated_revenue.estimated_partner_transaction_revenue,
daily_ad_revenue.ad_type,
daily_ad_revenue.ad_Description,
daily_ad_revenue.gross_revenue,
daily_ad_revenue.gross_add_impressions
from
(
select
ytdo.channel_id,
ytdo.video_id,
ytdo.report_date,
ytdo.country_code,
ytdo.uploader_type,
sum(ytdo.views) views,
sum(ytdo.watch_time_minutes) as watch_time_minutes,
sum(ytdo.red_views) as red_views,
sum(ytdo.red_watch_time_minutes) as red_watch_time_minutes
from fds_yt.youtube_device_and_os ytdo
where report_date between current_date - 68  and current_date - 1 
group by
ytdo.channel_id,
ytdo.video_id,
ytdo.report_date,
ytdo.country_code,
ytdo.uploader_type 
) REV_CONSUMPTION
full outer join
(select
channel_id,
CHANNEL_ID AS REV_CHANNEL_ID,
video_id,
uploader_type,
country_code,
report_date,
sum(estimated_partner_revenue) as estimated_partner_revenue,
sum(estimated_partner_ad_revenue) as estimated_partner_ad_revenue,
sum(estimated_partner_ad_auction_revenue) as estimated_partner_ad_auction_revenue,
sum(estimated_partner_ad_reserved_revenue) as estimated_partner_ad_reserved_revenue,
sum(estimated_youtube_ad_revenue) as estimated_youtube_ad_revenue,
SUM(estimated_monetized_playbacks) as estimated_monetized_playbacks,
sum(ad_impressions) as ad_impressions,
sum(estimated_partner_red_revenue) as estimated_partner_red_revenue,
sum(estimated_partner_transaction_revenue) as estimated_partner_transaction_revenue
from
fds_yt.youtube_estimated_revenue
where report_date between current_date - 68  and current_date - 1 
group by
channel_id,
video_id,
uploader_type,
country_code,
report_date) daily_estimated_revenue
ON
REV_CONSUMPTION.channel_id = daily_estimated_revenue.channel_id and
REV_CONSUMPTION.video_id = daily_estimated_revenue.video_id and
REV_CONSUMPTION.uploader_type = daily_estimated_revenue.uploader_type and
REV_CONSUMPTION.country_code = daily_estimated_revenue.country_code and
REV_CONSUMPTION.report_date = daily_estimated_revenue.report_date
full outer join
(select
channel_id,
video_id,
uploader_type,
country_code,
report_date,
ad_type,
case WHEN ad_type=1 THEN ' skippable_auction' 
      WHEN ad_type=2 THEN 'display_auction'
      WHEN ad_type=3 THEN 'non_skippable_auction'
      WHEN ad_type=5 THEN 'display_reserved'
      WHEN ad_type=6 THEN 'non_skippable_reserved'
      WHEN ad_type=15 THEN 'skippable_reserved'
      WHEN ad_type=19 THEN 'bumper_auction'
      WHEN ad_type=20 THEN 'bumper_reserved'
      WHEN ad_type=13 THEN 'unknown'
 END ad_Description,
--fds_yt.dim_advertisement.advertisement_description,
sum(estimated_youtube_ad_revenue) as gross_revenue,
sum( ad_impressions ) as      gross_add_impressions
from
fds_yt.youtube_ad_rates
where report_date between current_date - 68  and current_date - 1 
group by
channel_id,
video_id,
uploader_type,
country_code,
report_date,
ad_type) daily_ad_revenue
ON
REV_CONSUMPTION.channel_id = daily_ad_revenue.channel_id and
REV_CONSUMPTION.video_id = daily_ad_revenue.video_id and
REV_CONSUMPTION.uploader_type = daily_ad_revenue.uploader_type and
REV_CONSUMPTION.country_code = daily_ad_revenue.country_code and
REV_CONSUMPTION.report_date = daily_ad_revenue.report_date
) yt_daily_revenue

left outer join fds_yt.dim_channel dc
on dc.channel_id = yt_daily_revenue.channel_id

left outer join fds_yt.dim_video dv
on dv.video_id = yt_daily_revenue.video_id

left outer join fds_yt.dim_country_region dco
on dco.country_code = yt_daily_revenue.country_code

insert into dwh_read_write.rpt_yt_revenue_daily_ad_type

SELECT channel_id, channel_name, video_id, title,report_date, to_date(report_date,'YYYYMMDD') report_date_key,  country_code,
country_name, region, ad_region, uploader_type, views, watch_time_minutes, red_views,
red_watch_time_minutes, estimated_partner_revenue, estimated_partner_ad_revenue,
estimated_partner_ad_auction_revenue, estimated_partner_ad_reserved_revenue, estimated_youtube_ad_revenue,
estimated_monetized_playbacks, ad_impressions, estimated_partner_red_revenue,
estimated_partner_transaction_revenue, ad_type, ad_description, gross_revenue, gross_add_impressions,
insert_timestamp, 1000010 FROM #Temp

select count(*) from dwh_read_write.rpt_yt_revenue_daily_ad_type




