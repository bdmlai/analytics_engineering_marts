{{
  config({
		"materialized": 'ephemeral'
  })
}}
select 
c.country,
a.date,
sum(a.fb_followers) as fb_followers,
sum(b.igm_followers) as igm_followers,
sum(d.yt_followers) as yt_followers
from
(select 
trunc(date_trunc('month',date)) as date,
dim_country_id,
sum(followers) as fb_followers
from
(select dim_country_id, date, followers
from
(select dim_country_id,trunc(convert(datetime,convert(varchar(10),as_on_date))) as date,
sum(c_followers)as followers from fds_fbk.fact_fb_smfollowership_audience_bycountry group by 1,2))
where followers is not null group by 1,2 order by 1,2 desc) as a
left join 

/*Instagram Followers */--ephemeral
(select * from {{ref('intm_instagram_followers')}}) as b
on a.date=b.date and 
a.dim_country_id=b.dim_country_id

left join --ephemeral country_population
(select * from {{ref('intm_country_population')}}) as c
on a.dim_country_id = c.dim_country_id

left join

/*YouTube Followers */--ephemearl
(select * from {{ref('intm_youtube_followers')}}) as d 
on a.date = d.date and
upper(c.country)=upper(d.country)
group by 1,2
order by 1,2