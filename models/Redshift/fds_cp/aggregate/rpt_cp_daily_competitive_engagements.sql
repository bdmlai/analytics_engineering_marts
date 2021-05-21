/*
***************************************************************************************************
**********************************************
Date        : 04/29/2021
Version     : 1.0
TableName   : rpt_cp_daily_competitive_engagements
Schema    	: fds_cp
Contributor : Sandeep Battula
Description : This table has the engagement data for competitibve brands from platforms - facebook , instagram,
 twitter and youtube
***************************************************************************************************
**********************************************
*/
{{ config({ "schema": 'fds_cp', "materialized": 'table','tags': "Brand", "persist_docs": {
'relation' : true, 'columns' : true}, "post-hook": ["grant select on {{this}} to public"] }) }}

SELECT
    'Facebook'                    AS platform,
    b.name                        AS brand,
    TRUNC(page_metrics_date)      AS as_on_date,
    COALESCE(SUM(interactions),0) AS Engagements,
    100001                        AS etl_batch_id,
    'bi_dbt_user_prd'             AS etl_insert_user_id,
    SYSDATE                       AS etl_insert_rec_dttm,
    ''                            AS etl_update_user_id,
    SYSDATE                       AS etl_update_rec_dttm
FROM
    {{source('hive_udl_sb','sb_daily_facebook_page_metrics')}} a
JOIN
    {{source('hive_udl_sb','sb_daily_social_platform_profile')}} b
ON
    a.id=b.id
AND LOWER(b.platform)='facebook'
WHERE
    a.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_facebook_page_metrics')}})
AND b.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
        WHERE
            platform ='facebook')
GROUP BY
    1,2,3,5,6,7,8,9

UNION ALL


SELECT
    'Instagram'                   AS platform,
    b.name                        AS brand,
    TRUNC(page_metrics_date)      AS as_on_date,
    COALESCE(SUM(interactions),0) AS Engagements,
    100001                        AS etl_batch_id,
    'bi_dbt_user_prd'             AS etl_insert_user_id,
    SYSDATE                       AS etl_insert_rec_dttm,
    ''                            AS etl_update_user_id,
    SYSDATE                       AS etl_update_rec_dttm
FROM
    {{source('hive_udl_sb','sb_daily_instagram_page_metrics')}} a
JOIN
    {{source('hive_udl_sb','sb_daily_social_platform_profile')}} b
ON
    a.id=b.id
AND LOWER(b.platform)='instagram'
WHERE
    a.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_instagram_page_metrics')}})
AND b.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
        WHERE
            platform ='instagram')
GROUP BY
    1,2,3,5,6,7,8,9


UNION ALL


SELECT
    'Twitter'                     AS platform,
    b.name                        AS brand,
    TRUNC(page_metrics_date)      AS as_on_date,
    COALESCE(SUM(interactions),0) AS Engagements,
    100001                        AS etl_batch_id,
    'bi_dbt_user_prd'             AS etl_insert_user_id,
    SYSDATE                       AS etl_insert_rec_dttm,
    ''                            AS etl_update_user_id,
    SYSDATE                       AS etl_update_rec_dttm
FROM
    {{source('hive_udl_sb','sb_daily_twitter_page_metrics')}} a
JOIN
    {{source('hive_udl_sb','sb_daily_social_platform_profile')}} b
ON
    a.id=b.id
AND LOWER(b.platform)='twitter'
WHERE
    a.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_twitter_page_metrics')}})
AND b.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
        WHERE
            platform ='twitter')
GROUP BY
    1,2,3,5,6,7,8,9


UNION ALL


SELECT
    'Youtube'                      AS platform,
    author_name                    AS brand,
    to_date(as_on_date,'yyyymmdd')    as_on_date,
    COALESCE(SUM(interactions),0)  AS Engagements,
	100001                         AS etl_batch_id,
    'bi_dbt_user_prd'              AS etl_insert_user_id,
    SYSDATE                        AS etl_insert_rec_dttm,
    ''                             AS etl_update_user_id,
    SYSDATE                        AS etl_update_rec_dttm
FROM
    {{source('hive_udl_sb','sb_daily_youtube_post_metrics')}} a
GROUP BY
    1,2,3,5,6,7,8,9

