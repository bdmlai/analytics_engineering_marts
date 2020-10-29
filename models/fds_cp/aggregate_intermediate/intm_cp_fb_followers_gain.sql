{{
  config({
		"materialized": 'ephemeral'
  })
}}

select
trunc(date_trunc('month',date)) as date,
dim_country_id,
sum(diff) as fb_gain,
sum(followers) as fb_followers
from
(select dim_country_id, date, followers,
followers-case when lag(followers) over (partition by dim_country_id order by date)
is null then 0 else lag(followers) over (partition by dim_country_id order by date) end as diff
from
(select dim_country_id,trunc(convert(datetime,convert(varchar(10),as_on_date))) as date,
sum(c_followers)as followers from fds_fbk.fact_fb_smfollowership_audience_bycountry group by 1,2))
where followers is not null group by 1,2 order by 1,2 desc