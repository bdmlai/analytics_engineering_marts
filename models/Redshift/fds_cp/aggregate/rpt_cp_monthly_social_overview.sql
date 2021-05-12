/*
*************************************************************************************************************************************************
   Date        : 11/13/2020
   Version     : 1.0
   TableName   : rpt_cp_monthly_social_overview
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Monthly Social reporting table consists of social consumption, engagemenet and followership data for social platforms. It also has corresponding account name, country and region details  
*************************************************************************************************************************************************
*************************************************************************************************************************************************
   Date        : 05/05/2021
   Version     : 2.0
   TableName   : rpt_cp_monthly_social_overview
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Enhancements done to the Monthly Social reporting table to add new metric - 'Engagement_per_1k' for social platforms - Facebook, Instagram and Twitter.   
*************************************************************************************************************************************************
*/
{{
  config({
		'schema': 'fds_cp',
		"materialized": 'incremental','tags': "Content","unique_key":"month",
		"post-hook": ["grant select on {{this}} to public"]
  })
}}


with #cp_social_final_data as 
(select month, 'Engagement_per_1k' as metric, platform, account, region, country, 
round((cast((sum(Engagement) * 1000) as float)/(sum(Post_Volume) * sum(Followership))),2) as value
from 
(select month, platform, account, region, country, 
case when Metric='Followers' then nullif(value,0) end as followership,
case when Metric = 'Post Volume' then nullif(value,0) end as post_volume,
case when Metric='Engagement' then value end as engagement
from {{ ref('intm_cp_social_overview') }}) a
group by 1,2,3,4,5,6
union all
select * from {{ ref('intm_cp_social_overview') }}) 


select 	distinct a.* , 
		b.value as prev_month,
		c.value as prev_year,
		100001 		 as  etl_batch_id,
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate 	 as etl_insert_rec_dttm,
		'' 			 as etl_update_user_id,
		sysdate 	 as etl_update_rec_dttm
from 	#cp_social_final_data a 
left join {{source('fds_cp','rpt_cp_monthly_social_overview')}} b
on a.month = add_months(b.month,1)
and a.metric = b.metric
and a.platform = b.platform
and a.account = b.account
left join {{source('fds_cp','rpt_cp_monthly_social_overview')}} c
on a.month = add_months(c.month,12)
and a.metric = c.metric
and a.platform = c.platform
and a.account = c.account
{% if is_incremental() %}
	where a.month = trunc(date_add('month',-1,date_trunc('month',current_date)))
{% endif %}
