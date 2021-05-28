/*
*************************************************************************************************************************************************
   TableName   : final_cpg_daily_prod_plcmnt_output
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : Final product placement analysis model
   Version      Date            Author               Request
   1.0          03/04/2021      schakraborty         ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='dt_stage',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','airdate','adj_time'],
                    post_hook = "grant select on {{ this }} to DA_SNAYAK_USER_ROLE"
 ) }}
with source_data AS 
    (SELECT airdate,
         dt_time,
         logentryguid,
         show,
         typeofshow,
         segmenttype,
         inpoint,
         upper(talent_listed) AS talent_listed,
         upper(talent_identified) AS talent_identified,
         min_pgm_val,
         adj_time from
        (SELECT airdate,
         airdate||' '||adj_time AS dt_time, logentryguid, show, typeofshow, segmenttype, inpoint, talent_listed, talent_identified, min_pgm_val, adj_time, adj_min from
            (SELECT a.*,
         b.talent_listed,
         talent_identified,
        
                CASE
                WHEN mins+min_pgm_val<=60 THEN
                mins+min_pgm_val-1
                ELSE mins+min_pgm_val-61
                END AS adj_min,
                CASE
                WHEN mins+min_pgm_val<=60 THEN
                hour
                ELSE hour+1
                END AS adj_hour,
                CASE
                WHEN len(adj_min)=2 THEN
                adj_hour||':'||adj_min
                WHEN len(adj_min)=1 THEN
                adj_hour||':0'||adj_min
                END AS adj_time
            FROM {{ref('summ_cpg_daily_prod_plcmnt_minute_program_final')}} a
            LEFT JOIN 
                (SELECT logentryguid,
         listagg(comments,
        '|') within group (order by comments) AS talent_listed
                FROM {{ref('intm_cpg_daily_prod_plcmnt_split_stage4_update')}}
                GROUP BY  logentryguid
                ORDER BY  logentryguid)b
                    ON a.logentryguid=b.logentryguid
                LEFT JOIN 
                    (SELECT logentryguid,
         comments AS talent_identified
                    FROM {{ref('intm_cpg_daily_prod_plcmnt_split_stage4_update')}}
                    GROUP BY  1,2
                    ORDER BY  logentryguid)c
                        ON a.logentryguid=c.logentryguid
                    ORDER BY  logentryguid,adj_time)
                    GROUP BY  1,2,3,4,5,6,7,8,9,10,11,12)
                    ORDER BY  logentryguid,adj_time )
                SELECT *
            FROM source_data 