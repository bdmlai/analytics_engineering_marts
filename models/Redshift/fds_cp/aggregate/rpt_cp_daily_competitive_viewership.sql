/*
***************************************************************************************************
**********************************************
Date        : 04/29/2021
Version     : 1.0
TableName   : rpt_cp_daily_competitive_viewership
Schema    	: fds_cp
Contributor : Sandeep Battula
Description : This table has the viewership data for competitibve brands from platforms - facebook , instagram,
 and youtube
***************************************************************************************************
**********************************************
*/
{{ config({ "schema": 'fds_cp', "materialized": 'table','tags': "Brand", "persist_docs": {
'relation' : true, 'columns' : true}, "post-hook": ["grant select on {{this}} to public"] }) }}

SELECT
    'Facebook'                       AS platform,
    b.name                           AS brand,
    to_date(a.as_on_date,'yyyymmdd')    as_on_date,
    CASE
        WHEN b.profile_labels LIKE ('%Sports Brand Social Report%')
        OR  b.profile_labels LIKE ('%sports%')
        THEN 'Sports'
        ELSE 'Regular'
    END                       AS category,
    0                         AS view_time,
    SUM(insights_video_views) AS views,
    100001                    AS etl_batch_id,
    'bi_dbt_user_prd'         AS etl_insert_user_id,
    SYSDATE                   AS etl_insert_rec_dttm,
    ''                        AS etl_update_user_id,
    SYSDATE                   AS etl_update_rec_dttm
FROM
    {{source('hive_udl_sb','sb_daily_facebook_post_insights')}} a
JOIN
    {{source('hive_udl_sb','sb_daily_social_platform_profile')}} b
ON
    split_part(a.id, '_',1)=b.id
AND LOWER(b.platform)='facebook'
WHERE
    b.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
        WHERE
            platform ='facebook')
GROUP BY
    1,2,3,4,5,7,8,9,10,11


UNION ALL


SELECT
    'Instagram'                      AS platform,
    b.name                           AS brand,
    to_date(a.as_on_date,'yyyymmdd')    as_on_date,
    CASE
        WHEN b.profile_labels LIKE ('%Sports Brand Social Report%')
        OR  b.profile_labels LIKE ('%sports%')
        THEN 'Sports'
        ELSE 'Regular'
    END                       AS category,
    0                         AS view_time,
    SUM(insights_video_views) AS views,
    100001                    AS etl_batch_id,
    'bi_dbt_user_prd'         AS etl_insert_user_id,
    SYSDATE                   AS etl_insert_rec_dttm,
    ''                        AS etl_update_user_id,
    SYSDATE                   AS etl_update_rec_dttm
FROM
    {{source('hive_udl_sb','sb_daily_instagram_post_insights')}} a
JOIN
    {{source('hive_udl_sb','sb_daily_social_platform_profile')}} b
ON
    a.profile_id=b.id
AND LOWER(b.platform)='instagram'
WHERE
    b.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
        WHERE
            platform ='instagram')
GROUP BY
    1,2,3,4,5,7,8,9,10,11


UNION ALL


SELECT
    'Youtube'                        AS platform,
    author_name                      AS brand,
    to_date(a.as_on_date,'yyyymmdd')    as_on_date,
    CASE
        WHEN b.profile_labels LIKE ('%Sports Brand Social Report%')
        OR  b.profile_labels LIKE ('%sports%')
        THEN 'Sports'
        ELSE 'Regular'
    END                  AS category,
    SUM(video_view_time) AS view_time,
    SUM(video_views)     AS views,
    100001               AS etl_batch_id,
    'bi_dbt_user_prd'    AS etl_insert_user_id,
    SYSDATE              AS etl_insert_rec_dttm,
    ''                   AS etl_update_user_id,
    SYSDATE              AS etl_update_rec_dttm
FROM
    {{source('hive_udl_sb','sb_daily_youtube_post_metrics')}} a
JOIN
    {{source('hive_udl_sb','sb_daily_social_platform_profile')}} b
ON
    a.author_name=b.name
AND LOWER(b.platform)='youtube'
WHERE
    b.as_on_date=
    (
        SELECT
            MAX(as_on_date)
        FROM
            {{source('hive_udl_sb','sb_daily_social_platform_profile')}}
        WHERE
            platform ='youtube')
GROUP BY
    1,2,3,4,7,8,9,10,11
