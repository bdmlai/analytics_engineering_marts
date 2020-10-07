

  create view "entdwdb"."fds_cp"."vw_aggr_cp_monthly_social_followership__dbt_tmp" as (
    

with __dbt__CTE__intm_cp_fb_followers_gain as (


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
),  __dbt__CTE__intm_cp_ig_followers_gain as (


select
trunc(date_trunc('month',date)) as date,
dim_country_id,
sum(diff) as igm_gain,
sum(followers) as igm_followers
from
(select  date, dim_country_id,followers,
followers-case when lag(followers) over (partition by dim_country_id order by date)
is null then 0 else lag(followers) over (partition by dim_country_id order by date) end as diff
from
(select dim_country_id,trunc(convert(datetime,convert(varchar(10),as_on_date))) as date,
sum(c_followers)as followers from fds_igm.fact_ig_smfollowership_audience_bycountry group by 1,2))
where followers is not null group by 1,2 order by 1,2 desc
),  __dbt__CTE__intm_cp_country_population as (


select dim_country_id,case when country_name='USA' then 'United States' else country_name end as Country, 
sum(population) as Population from cdm.dim_country_population group by 1,2
),  __dbt__CTE__intm_cp_yt_followers_gain as (


select 
trunc(date_trunc('month',trunc(convert(datetime,convert(varchar(10),report_date))))) as date,
country_name2 as country,
region2 as region,
sum(subscribers_gained) as yt_gain,
sum(subscribers_gained) - sum(subscribers_lost) as yt_followers
from fds_yt.agg_yt_monetization_summary
group by 1,2,3
order by 1,2,3 desc
)select 
c.country,
a.date,
avg(c.population) as population,
sum(a.fb_gain) as fb_gain,
sum(a.fb_followers) as fb_followers,
sum(b.igm_gain) as igm_gain,
sum(b.igm_followers) as igm_followers,
sum(d.yt_gain) as yt_gain,
sum(d.yt_followers) as yt_followers
from
(select date,dim_country_id,fb_gain,fb_followers from __dbt__CTE__intm_cp_fb_followers_gain )a
 left join 
(select date,dim_country_id,igm_followers,igm_gain from __dbt__CTE__intm_cp_ig_followers_gain )b 
 on  a.date=b.date and a.dim_country_id=b.dim_country_id
 left join 
(select dim_country_id ,Country,Population from __dbt__CTE__intm_cp_country_population )c
 on  a.dim_country_id = c.dim_country_id
 left join
(select date,country,region,yt_gain,yt_followers from  __dbt__CTE__intm_cp_yt_followers_gain )d
 on  a.date = d.date and upper(c.country)=upper(d.country)
group by 1,2
order by 1,2
  ) ;
