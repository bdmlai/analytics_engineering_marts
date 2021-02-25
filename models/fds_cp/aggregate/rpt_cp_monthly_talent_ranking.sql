/*
*************************************************************************************************************************************************
   Date        : 02/01/2021
   Version     : 1.0
   TableName   : rpt_cp_monthly_talent_ranking
   Schema	   : fds_cp
   Contributor : Sandeep Battula
   Description : Monthly Talent Ranking reporting table consists of social consumption, engagemenet, followership data and also merchandise sales data for the talents. Social metrics are fetched for platforms- Youtube Facebook, Instagram and Twitter. It also has corresponding brand, designation and gender details
*************************************************************************************************************************************************
*/

{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "delete from fds_cp.rpt_cp_monthly_talent_ranking where month = date_trunc('month', current_date-28)",
		"materialized": 'incremental'
		})
}}

with #merch_sales_final as 
(select talent_description as talent, trunc(date_trunc('month',date)) as month, 'Merch Sales' as platform, business_unit as metric, 
sum(demand_sales) as value
from {{ref('merch_sales')}}
group by 1,2,3,4
union all
select talent_description as talent, trunc(date_trunc('month',date)) as month, 'Merch Sales' as platform, 'Total' as metric, 
sum(demand_sales) as value
from {{ref('merch_sales')}}
group by 1,2,3,4
),

#cp_talent_data as
(select * from {{ref('yt_consumption_monthly')}}
union all
select * from {{ref('cp_talent_latest_data')}}
union all
select * from #merch_sales_final
),

#latest_emm_brand as 
(select a.*, 
case when (current_date - 30) between start_date and coalesce (end_date, current_date) then 'Active'
else 'Inactive' 
end as status from
(select * from (select character_lineage_wweid, designation, start_date, end_date, row_number() over (partition by character_lineage_wweid order by start_date desc) as row_num
from {{source('fds_emm','brand')}})
where row_num=1) a ),

#latest_emm_designation as 
(select * from (select character_lineage_wweid, designation, row_number() over (partition by character_lineage_wweid order by start_date desc) as row_num
from {{source('fds_emm','babyface_heel')}} where (current_date - 30) between start_date and coalesce (end_date, current_date))
where row_num=1)


select distinct A.month, A.platform, A.metric, 
A.value,
B.all_conviva_accounts as talent,
coalesce(E.designation, 'Other') as brand,
INITCAP(coalesce(D.designation, 'Other')) as designation,
INITCAP(coalesce(C.gender, 'Unavailable')) as gender,
coalesce(E.status, 'Unavailable') as status,
 100001 as  etl_batch_id,
sysdate etl_insert_rec_dttm,
'' etl_update_user_id,
sysdate etl_update_rec_dttm,
'bi_dbt_user_prd' as etl_insert_user_id
from 	#cp_talent_data A
join {{source('hive_udl_cp','da_monthly_conviva_emm_accounts_mapping')}} B
on lower(A.talent) = lower(B.all_conviva_accounts)
left join {{source('fds_mdm','character')}} C
on coalesce (B.character_lineage,'NA') = coalesce (C.character_lineage_name,'NA')
and B.character_lineage <> ' '
Left Join #latest_emm_designation D 
on c.character_lineages_wweid=d.character_lineage_wweid
left join #latest_emm_brand E
on c.character_lineages_wweid=e.character_lineage_wweid
{% if is_incremental() %}
where month=date_trunc('month', current_date-28)
{% endif %}