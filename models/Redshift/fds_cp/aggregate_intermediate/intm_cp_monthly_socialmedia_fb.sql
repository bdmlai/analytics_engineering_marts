{{
  config({
		"materialized": 'ephemeral'
  })
}}


select trunc(add_months(to_date(as_on_date,'YYYYMMDD'), -1)) as month,src_country_name as country,
'facebook_followers' as metric,
'NA' as page,
total as values,'Social Media' as platform
from
(
select a.as_on_date,c.src_country_name, sum(a.c_followers) as total 
from {{source('fds_fbk','fact_fb_smfollowership_audience_bycountry')}} as a
left join {{source('cdm','dim_country')}} as c on a.dim_country_id=c.dim_country_id
where a.as_on_date like '%01' 
group by 1,2
)
union all
-- adding facebook local page followers metric ---
select trunc(add_months(to_date(as_on_date,'YYYYMMDD'), -1)) as month, country,
'facebook_local_page_followers' as metric, page,
total as values,'Social Media' as platform
from
(
select a.as_on_date,c.country, page, sum(a.c_followers) as total 
from {{source('fds_fbk','fact_fb_smfollowership_audience_bycountry')}} as a
left join 
(select dim_smprovider_account_id,channel_handle_name as page, as_on_date, country_market as country 
from {{source('hive_udl_cp','daily_social_account_country_mapping')}}
where platform = 'Facebook') as c 
on a.dim_smprovider_account_id=c.dim_smprovider_account_id and a.as_on_date = c.as_on_date
where a.as_on_date like '%01' 
group by 1,2,3
) where country is not null