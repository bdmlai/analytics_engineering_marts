{{
  config({
		"schema": 'udl_ads',
		"materialized": 'incremental'
  })
}}


select *,
split_part(split_part(customtargeting,'app_name=',2),';',1) as app_name,
 split_part(split_part(customtargeting,'app_version=',2),';',1) as app_version,
split_part( split_part(customtargeting,'autoplay=',2),';',1) as autoplay,
split_part( split_part(customtargeting,'chained=',2),';',1) as chained,
split_part( split_part(customtargeting,'conviva_stream=',2),';',1) as conviva_stream,
split_part( split_part(customtargeting,'envmt=',2),';',1) as envmt,
split_part( split_part(customtargeting,'ex_id=',2),';',1) as ex_id,
split_part( split_part(customtargeting,'last_video=',2),';',1) as last_video,
 split_part(split_part(customtargeting,'milestone_start=',2),';',1) as milestone_start,
 split_part(split_part(customtargeting,'pluid=',2),';',1) as pluid ,
  split_part(split_part(customtargeting,'resume=',2),';',1) as resume,
 split_part(split_part(customtargeting,'user_tier=',2),';',1) as user_tier 
from HIVE_udl_Ads.google_daily_networkimpressions
where adunitid in ('21954673660','21954672988')
and time >= '2021-03-15 00:00:00'
