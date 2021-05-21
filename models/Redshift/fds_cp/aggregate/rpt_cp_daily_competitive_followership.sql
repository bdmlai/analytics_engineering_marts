/*
***************************************************************************************************
**********************************************
Date        : 04/29/2021
Version     : 1.0
TableName   : rpt_cp_daily_competitive_followership
Schema   	: fds_cp
Contributor : Sandeep Battula
Description : This table has the followership data for competitibve brands from platforms - facebook , instagram,
 twitter and youtube
***************************************************************************************************
**********************************************
*/
{{ config({ "schema": 'fds_cp', "materialized": 'table','tags': "Brand", "persist_docs": {
'relation' : true, 'columns' : true}, "post-hook": ["grant select on {{this}} to public"] }) }}

SELECT DISTINCT
    'Facebook'               AS platform,
    b.name                   AS brand,
    TRUNC(page_metrics_date) AS as_on_date,
    NULL                     AS country_cd,
    'Global'                 AS country_nm,
    'Global'                 AS region_nm,
    MAX(a.fans_lifetime)     AS followers,
    100001                   AS etl_batch_id,
    'bi_dbt_user_prd'        AS etl_insert_user_id,
    SYSDATE                  AS etl_insert_rec_dttm,
    ''                       AS etl_update_user_id,
    SYSDATE                  AS etl_update_rec_dttm
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
AND a.fans_lifetime IS NOT NULL
GROUP BY
    1,2,3,4,5,6,8,9,10,11,12


UNION ALL


SELECT DISTINCT
    'Instagram'               AS platform,
    b.name                    AS brand,
    TRUNC(page_metrics_date)  AS as_on_date,
    NULL                      AS country_cd,
    'Global'                  AS country_nm,
    'Global'                  AS region_nm,
    MAX(a.followers_lifetime) AS followers,
    100001                    AS etl_batch_id,
    'bi_dbt_user_prd'         AS etl_insert_user_id,
    SYSDATE                   AS etl_insert_rec_dttm,
    ''                        AS etl_update_user_id,
    SYSDATE                   AS etl_update_rec_dttm
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
AND a.followers_lifetime IS NOT NULL
GROUP BY
    1,2,3,4,5,6,8,9,10,11,12


UNION ALL


SELECT DISTINCT
    'Twitter'                 AS platform,
    b.name                    AS brand,
    TRUNC(page_metrics_date)  AS as_on_date,
    NULL                      AS country_cd,
    'Global'                  AS country_nm,
    'Global'                  AS region_nm,
    MAX(a.followers_lifetime) AS followers,
    100001                    AS etl_batch_id,
    'bi_dbt_user_prd'         AS etl_insert_user_id,
    SYSDATE                   AS etl_insert_rec_dttm,
    ''                        AS etl_update_user_id,
    SYSDATE                   AS etl_update_rec_dttm
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
AND a.followers_lifetime IS NOT NULL
GROUP BY
    1,2,3,4,5,6,8,9,10,11,12


UNION ALL


SELECT DISTINCT
    'Youtube'                         AS platform,
    b.name                            AS brand,
    TRUNC(page_metrics_date)          AS as_on_date,
    NULL                              AS country_cd,
    'Global'                          AS country_nm,
    'Global'                          AS region_nm,
    MAX(a.subscribers_count_lifetime) AS followers,
    100001                            AS etl_batch_id,
    'bi_dbt_user_prd'                 AS etl_insert_user_id,
    SYSDATE                           AS etl_insert_rec_dttm,
    ''                                AS etl_update_user_id,
    SYSDATE                           AS etl_update_rec_dttm
FROM
    {{source('hive_udl_sb','sb_daily_youtube_page_metrics')}} a
JOIN
    {{source('hive_udl_sb','sb_daily_social_platform_profile')}} b
ON
    a.id=b.id
AND LOWER(b.platform)='youtube'
WHERE
    a.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_youtube_page_metrics')}})
AND b.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
        WHERE
            platform ='youtube')
AND a.subscribers_count_lifetime IS NOT NULL
GROUP BY
    1,2,3,4,5,6,8,9,10,11,12


UNION ALL


SELECT
    'Facebook' AS platform,
    name       AS brand,
    as_on_date,
    UPPER(country_cd) AS country_cd,
    country_nm,
    region_nm,
    AVG(followers)    AS followers,
    100001            AS etl_batch_id,
    'bi_dbt_user_prd' AS etl_insert_user_id,
    SYSDATE           AS etl_insert_rec_dttm,
    ''                AS etl_update_user_id,
    SYSDATE           AS etl_update_rec_dttm
FROM
    (
        SELECT DISTINCT
            c.name,
            TRUNC(page_insights_date) AS as_on_date,
            b.iso_alpha2_ctry_cd      AS country_cd,
            b.country_nm,
            b.region_nm,
            JSON_EXTRACT_PATH_TEXT(insights_fans_lifetime_by_country, UPPER(b.iso_alpha2_ctry_cd))
            AS followers
        FROM
            {{source('hive_udl_sb','sb_daily_facebook_page_insights')}} a
        JOIN
            {{source('cdm','dim_region_country')}} b
        ON
            b.src_sys_cd='iso'
        AND b.ent_map_nm='GM Region'
        JOIN
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}} c
        ON
            a.id=c.id
        AND LOWER(c.platform)='facebook'
        WHERE
            a.as_on_date=
            (
                SELECT
                    MAX(as_on_date)
                FROM
                    {{source('hive_udl_sb','sb_daily_facebook_page_insights')}})
        AND c.as_on_date=
            (
                SELECT
                    MAX(as_on_date)
                FROM
                    {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
                WHERE
                    platform ='facebook')
        AND followers >= 0 )
GROUP BY
    1,2,3,4,5,6,8,9,10,11,12


UNION ALL


SELECT
    'Instagram' AS platform,
    name        AS brand,
    as_on_date,
    UPPER(country_cd) AS country_cd,
    country_nm,
    region_nm,
    AVG(followers)    AS followers,
    100001            AS etl_batch_id,
    'bi_dbt_user_prd' AS etl_insert_user_id,
    SYSDATE           AS etl_insert_rec_dttm,
    ''                AS etl_update_user_id,
    SYSDATE           AS etl_update_rec_dttm
FROM
    (
        SELECT DISTINCT
            c.name,
            TRUNC(page_insights_date) AS as_on_date,
            b.iso_alpha2_ctry_cd      AS country_cd,
            b.country_nm,
            region_nm,
            JSON_EXTRACT_PATH_TEXT(insights_followers_by_country_by_day, UPPER(b.iso_alpha2_ctry_cd
            )) AS followers
        FROM
            {{source('hive_udl_sb','sb_daily_instagram_page_insights')}} a
        JOIN
            {{source('cdm','dim_region_country')}} b
        ON
            b.src_sys_cd='iso'
        AND b.ent_map_nm='GM Region'
        JOIN
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}} c
        ON
            a.id=c.id
        AND LOWER(c.platform)='instagram'
        WHERE
            a.as_on_date=
            (
                SELECT
                    MAX(as_on_date)
                FROM
                    {{source('hive_udl_sb','sb_daily_instagram_page_insights')}})
        AND c.as_on_date=
            (
                SELECT
                    MAX(as_on_date)
                FROM
                    {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
                WHERE
                    platform ='instagram')
        AND followers >= 0 )
GROUP BY
    1,2,3,4,5,6,8,9,10,11,12
