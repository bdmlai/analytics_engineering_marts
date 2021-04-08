/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_appearance_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture screentime and appearance of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent equity','screentime','show','daily','centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(

select airdate,entity_type,case when entity_type='Character' then c.character_lineages_wweid when entity_type='Group' 
then d.character_lineage_wweid end as lineage_wweid, 
case when entity_type='Character' then c.character_lineage_name when entity_type='Group' 
then d.character_lineage_name end as lineage_name,typeofshow,title,segmenttype,matchtype,role,comment,
((substring(duration,1,2) * 360) + (substring(duration,4,2) * 60) + (substring(duration,7,2) * 1)) as tot_dur
from {{source('sf_udl_nplus','raw_lite_log')}} a inner join {{source('sf_udl_cp','drvd_emm_nplus_weekly_talent_breakout')}} b  
on a.logentryguid=b.logentryguid
left join {{source('sf_fds_mdm','character')}} c on b.talent_wweid=c.characters_wweid and entity_type='Character' 
left join {{source('sf_fds_mdm','groups')}} d on b.talent_wweid=d.group_wweid and entity_type='Group' 
where airdate>='2018-01-01' 
and duration>='00:00:00:00' 
group by 1,2,3,4,5,6,7,8,9,10,11
)
select * from 
    source_data 