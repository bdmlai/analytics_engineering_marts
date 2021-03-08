{{
  config({
		"materialized": 'ephemeral'
  })
}}

select b.cal_year as year,b.cal_mth_num as month,
d.account_name, 'TW' as platform,
(a.twitter_followers)  as followers
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}  a left join
{{source('cdm','dim_smprovider_account')}} d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id  left join
{{source('cdm','dim_date')}} b on a.dim_date_id = b.dim_date_id
 join
(
select b.cal_year as year,b.cal_mth_num as month,
d.account_name,max(a.dim_date_id) as last_day 
from {{source('fds_cp','fact_co_smfollowership_cumulative_summary')}}   a left join
{{source('cdm','dim_smprovider_account')}} d on a.dim_Smprovider_account_id = d.dim_smprovider_account_id left join
{{source('cdm','dim_date')}} b on a.dim_date_id = b.dim_date_id

group by 1,2,3
) h 
on month=h.month and 
year=h.year and
d.account_name=h.account_name
and a.dim_date_id=h.last_day
