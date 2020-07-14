

  create view "entdwdb"."fds_nl"."vw_rpt_nl_weekly_overlap_chart__dbt_tmp" as (
    /*
*************************************************************************************************************************************************
   Date        : 06/26/2020
   Version     : 1.0
   TableName   : vw_rpt_nl_weekly_overlap_chart
   Schema	   : fds_nl
   Contributor : Remya K Nair
   Description : vw_rpt_nl_weekly_overlap_chart view consists of Derived Columns for Overlap data for  WWE, AEW and other wrestling programs 
*************************************************************************************************************************************************
*/




SELECT
    as_on_date,
    primary_schedule_name             AS primary_schedule,
    secondary_schedule_name           AS secondary_schedule,
    src_market_break                  AS market_break,
    src_demographic_group             AS demographic_group,
    src_playback_period_cd            AS playback_period,
    primary_reach_proj000             AS Primary_Reach_Proj_000,
    combined_reach_proj000       	  AS Combined_Reach_Proj_000,
    primary_only_reach_proj000        AS Primary_Only_Reach_Proj_000,
    secondary_only_reach_proj000      AS secondary_only_reach_proj000,
    both_reach_proj000                AS both_reach_proj000,
    CASE
        WHEN secondary_schedule IN ('All but SD',
                                    'All but Raw',
                                    'All but NXT',
                                    'All but AEW',
                                    'All but WWE')
        THEN (primary_only_reach_proj000*1.0000)/(NULLIF(primary_reach_proj000*1.0000,0))*100
        ELSE (both_reach_proj000*1.0000)/(NULLIF(primary_reach_proj000*1.0000,0))*100
    END AS Percent_of_Primary_Show ,
    CASE
        WHEN secondary_schedule IN ('All but SD',
                                    'All but Raw',
                                    'All but NXT',
                                    'All but AEW',
                                    'All but WWE')
        THEN (primary_only_reach_proj000*1.0000)/(NULLIF(combined_reach_proj000*1.0000,0))*100
        ELSE (both_reach_proj000*1.0000)/(NULLIF(T.max_combined_reach_proj000*1.0000,0))*100
    END AS Percent_of_Wrestling,
    CASE
        WHEN secondary_schedule IN ('All but SD',
                                    'All but Raw',
                                    'All but NXT',
                                    'All but AEW',
                                    'All but WWE')
        THEN CAST(primary_only_reach_proj000 AS DECIMAL)
        ELSE CAST(both_reach_proj000 AS DECIMAL)
    END AS Overlap_Reach_Proj000
FROM "entdwdb"."fds_nl"."fact_nl_weekly_overlap_chart" a
  
INNER JOIN
    (
        SELECT
            dim_date_id,
            MAX(CAST(combined_reach_proj000 AS DECIMAL)) AS max_combined_reach_proj000
        FROM  "entdwdb"."fds_nl"."fact_nl_weekly_overlap_chart"
           
        GROUP BY
            dim_date_id )T
ON
    a.as_on_date=T.dim_date_id
    order by as_on_date
  ) ;
