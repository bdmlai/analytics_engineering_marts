
select account as talent,
		month, 
		platform,
		metric,
		sum(value) as value
from
(
select 	a.month, 
		a.metric,
		a.value,
		b.platform_description as platform,
		nvl(c.account_name,'Other') as account
from __dbt__CTE__cp_talent_platform_data a
inner join "entdwdb"."cdm"."dim_platform" as b
on 	a.dim_platform_id = b.dim_platform_id
left join "entdwdb"."cdm"."dim_smprovider_account" c
on  a.dim_smprovider_account_id = c.dim_smprovider_account_id 
and a.cp_dim_platform = c.dim_platform_id
and active_flag = 'true')
group by 1,2,3,4