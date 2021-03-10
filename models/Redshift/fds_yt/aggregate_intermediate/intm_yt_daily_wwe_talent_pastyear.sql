{{
  config({
		"materialized": 'ephemeral'
  })
}}

with cte as 
(select n::int from
  (select row_number() over (order by true) as n from {{ref('intm_yt_daily_wwe_video_pastyear')}})
cross join
(select  max(regexp_count(talent, '[,]')) as max_num from {{ref('intm_yt_daily_wwe_video_pastyear')}}) where n <= max_num + 1)
select distinct time_uploaded, trim(SPLIT_PART(talent,',',n)) as talent, video_id,
 region_code, ttl_views,views_30days
 from {{ref('intm_yt_daily_wwe_video_pastyear')}}
cross join cte
where  split_part(talent,',',n) is not null and split_part(talent,',',n) != ''