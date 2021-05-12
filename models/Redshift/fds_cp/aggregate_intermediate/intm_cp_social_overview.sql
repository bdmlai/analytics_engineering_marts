{{
  config({
		'schema': 'fds_cp',
		"materialized": 'ephemeral'
  })
}}


with #cp_social_platform_data as (
select * from {{ ref('intm_cp_social_post_volume') }} union all
select * from {{ ref('intm_cp_social_consumption') }} union all
select * from {{ ref('intm_cp_social_engagements') }} union all
select * from {{ ref('intm_cp_social_followers') }} )

select month, 
		metric,
		platform,
		account,
		region,
		country,
		sum(value) as value
from
(
select 	a.month, 
		a.metric,
		a.value,
		b.platform_description as platform,
		nvl(c.account_name,'Other') as account,
		nvl(d.wwe_region, 'Other') as region,
		nvl(d.country_market, 'Other') as country
from #cp_social_platform_data a
inner join {{source('cdm','dim_platform')}} as b
on 	a.dim_platform_id = b.dim_platform_id
left join {{source('cdm','dim_smprovider_account')}} c
on  a.dim_smprovider_account_id = c.dim_smprovider_account_id 
and a.cp_dim_platform = c.dim_platform_id
and active_flag = 'true'
left join 
(select * from {{source('hive_udl_cp','daily_social_account_country_mapping')}}
where as_on_date = (select max(as_on_date) from {{source('hive_udl_cp','daily_social_account_country_mapping')}})) d
on  a.dim_smprovider_account_id = d.dim_smprovider_account_id 
and a.cp_dim_platform = d.dim_platform_id)
group by 1,2,3,4,5,6