/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_achievement_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture achievement of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent equity','title','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

select case when lower(name) like '%champion%' then name else (name|| ' ' ||year(start_date)) end as title,
case when entity_type='characters' then 'Character' when entity_type='groups' 
then 'Group' end as entity_type,
case when entity_type='characters' then b.character_lineages_wweid when entity_type='groups' 
then c.character_lineage_wweid end as lineage_wweid, 
case when entity_type='characters' then b.character_lineage_name when entity_type='groups' 
then c.character_lineage_name end as lineage_name,to_date(start_date) as start_date,
case when end_date is null then current_date else to_date(end_date) end as end_date
from {{source('sf_fds_mdm','achievement_entity')}} a
left join {{source('sf_fds_mdm','character')}} b on a.entity_id=b.id and entity_type='characters'
left join {{source('sf_fds_mdm','groups')}} c on a.entity_id=c.mdmid and entity_type='groups' 
group by 1,2,3,4,5,6

)
select * from 
    source_data