/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model having cross join of all talents featuring in any WWE episode from 2018 onwards

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
PSTA-3481  05/21/2021   Code fix to eliminate lineage_wweid duplicities in fds_mdm.character table 
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['talent equity','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}
with source_data as

(
select * from (
select full_date as date,character_lineage_name as lineage_name,character_lineages_wweid as lineage_wweid,source_string,
case when entity_type='character_lineages' then 'Character' when entity_type='group_lineages' then 'Group' end as entity_type,
listagg(gender,'|') within group (order by full_date,character_lineage_name) as gender from(
select a.character_lineage_name,a.character_lineages_wweid,b.source_string,a.entity_type,gender from(
select characters_name,character_lineage_name,character_lineages_wweid,'character_lineages' as entity_type,
character_lineage_id as entity_id,gender from {{source('sf_fds_mdm','character')}} where enabled=true
union all
select group_name,character_lineage_name,character_lineage_wweid,'group_lineages' as entity_type,
character_lineage_mdmid as entity_id,null as gender from {{source('sf_fds_mdm','groups')}} where enabled=true)a
left join (select alternate_id_name,source_string,entity_type,entity_id from {{source('sf_fds_mdm','alternateid')}}
where alternate_id_type_name='Merch Sales' group by 1,2,3,4)b
on a.entity_type=b.entity_type and a.entity_id=b.entity_id group by 1,2,3,4,5)a
cross join(
select full_date from {{source('sf_cdm','dim_date')}} where full_date>= '2018-01-01' and full_date<=current_date group by 1 order by 1)b 
group by 1,2,3,4,5 order by 2,1) where lineage_name is not null

)

select * from 
    source_data 
