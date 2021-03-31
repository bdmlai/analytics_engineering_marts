
/*
*************************************************************************************************************************************************
   TableName   : base_daily_talent_tv_rating_table
   Schema	   : CREATIVE
   Contributor : Sourish Chakraborty
   Description : Base Model to capture TV Rating of talents

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',
            enabled = true, 
                tags = ['talent equity','TV Rating','daily',
                        'centralized table'],
                    post_hook = "grant select on {{ this }} to DA_YYANG_USER_ROLE"
 ) }}

with source_data as

(


select airdate,
case when lower(entity_type)='character' then 'characters' when lower(entity_type)='group' then 'groups' end as entity,
case when lower(entity_type)='character' then c.character_lineages_wweid
when lower(entity_type)='group' then e.character_lineage_wweid end as lineage_wweid,
case when lower(entity_type)='character' then c.character_lineage_name
when lower(entity_type)='group' then e.character_lineage_name end as lineage_name,
avg(most_current_us_audience_avg_proj_000) as us_aud
from {{source('sf_fds_nl','vw_rpt_nl_daily_minxmin_lite_log_ratings')}} a
inner join {{source('sf_udl_cp','drvd_emm_nplus_weekly_talent_breakout')}} b on a.logentryguid=b.logentryguid
left join {{source('sf_fds_mdm','character')}} c on b.talent_wweid=c.characters_wweid
left join {{source('sf_fds_mdm','groups')}} d on b.talent_wweid=d.group_wweid
left join {{source('sf_fds_mdm','characterlineage_grouplineage')}} e on d.character_lineage_name=e.group_lineage_name
where airdate>='2018-01-01' and src_demographic_group='Persons 2 - 99' group by 1,2,3,4


)
select * from 
    source_data