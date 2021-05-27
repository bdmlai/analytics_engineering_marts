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


with source_data AS 
    (SELECT *
    FROM 
        (SELECT full_date AS date,
        character_lineage_name AS lineage_name,
        character_lineages_wweid AS lineage_wweid,
        source_string,
        gender,
        
            CASE
            WHEN entity_type='character_lineages' THEN
            'Character'
            WHEN entity_type='group_lineages' THEN
            'Group'
            END AS entity_type from
            (SELECT a.character_lineage_name,
        a.character_lineages_wweid,
        b.source_string,
        a.entity_type,
        gender from
                (SELECT characters_name,
        character_lineage_name,
        character_lineages_wweid,
        'character_lineages' AS entity_type, character_lineage_id AS entity_id,gender
                FROM {{source('sf_fds_mdm','character')}}
                WHERE enabled=true --PSTA-3481 enabled
                UNION all
                SELECT group_name,
        character_lineage_name,
        character_lineage_wweid,
        'group_lineages' AS entity_type, character_lineage_mdmid AS entity_id,null AS gender
                FROM {{source('sf_fds_mdm','groups')}}
                WHERE enabled=true)a --PSTA-3481 enabled
                LEFT JOIN 
                    (SELECT alternate_id_name,
        source_string,
        entity_type,
        entity_id
                    FROM {{source('sf_fds_mdm','alternateid')}}
                    WHERE alternate_id_type_name='Merch Sales'
                    GROUP BY  1,2,3,4)b
                        ON a.entity_type=b.entity_type
                            AND a.entity_id=b.entity_id
                    GROUP BY  1,2,3,4,5)a cross join
                        (SELECT full_date
                        FROM {{source('sf_cdm','dim_date')}}
                        WHERE full_date>= '2018-01-01'
                                AND full_date<=current_date
                        GROUP BY  1
                        ORDER BY  1)b
                        GROUP BY  1,2,3,4,5,6
                        ORDER BY  2,1)
                        WHERE lineage_name is NOT NULL )
                    SELECT *
                FROM source_data 