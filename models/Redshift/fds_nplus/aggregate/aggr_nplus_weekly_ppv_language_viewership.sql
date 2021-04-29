--weekly PPV Alternate Language Viewership by Country
/*
***************************************************************************************************
**********************************************
Date        : 03/15/2021
Version     : 1.0
TableName   : aggr_nplus_weekly_ppv_language_viewership
Schema    : fds_nplus
Contributor : Sandeep Battula
Description : This table aggregates the viewership data for Pay per view (PPV) Events based on the
language and country. The metrics generated are Linear viewership, same day viewership, 3 day, 7
day and 30 day viewership
***************************************************************************************************
**********************************************
*/
--prehook - delete from fds_nplus.aggr_nplus_weekly_ppv_language_viewership where view_date=
-- (current_date-1)
{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'table','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": ["grant select on {{this}} to public"],
  })
}}
SELECT
    a.production_id,
    b.episode_nm,
    b.debut_date,
    a.language,
    a.country_cd,
    CASE
        WHEN LOWER(b.episode_nm) LIKE '%wrestle%'
        OR  LOWER(b.episode_nm) LIKE '%summer%'
        OR  LOWER(b.episode_nm) LIKE '%survivor%'
        OR  (LOWER(b.episode_nm) LIKE '%royal%'
            AND LOWER(b.episode_nm) NOT LIKE '%greatest%')
        THEN 'Tier 1'
        ELSE 'Tier 2'
    END                AS tier,
    (CURRENT_DATE - 1) AS view_date,
	min(a.dim_country_id) as dim_country_id,
    COUNT(DISTINCT (
        CASE
            WHEN stream_start_dttm BETWEEN live_start_schedule AND live_end_schedule
            AND stream_type_cd='linear'
            THEN src_fan_id
            ELSE NULL
        END)) live_unique_viewer_cnt,
    COUNT(DISTINCT (
        CASE
            WHEN stream_start_dttm BETWEEN first_stream AND _same_day
            THEN src_fan_id
            ELSE NULL
        END)) same_day_unique_viewer_cnt,
    COUNT(DISTINCT (
        CASE
            WHEN stream_start_dttm BETWEEN first_stream AND _3day_dt
            THEN src_fan_id
            ELSE NULL
        END)) _3_day_unique_viewer_cnt ,
    COUNT(DISTINCT (
        CASE
            WHEN stream_start_dttm BETWEEN first_stream AND _7day_dt
            THEN src_fan_id
            ELSE NULL
        END)) _7_day_unique_viewer_cnt ,
    COUNT(DISTINCT (
        CASE
            WHEN stream_start_dttm BETWEEN first_stream AND _30day_dt
            THEN src_fan_id
            ELSE NULL
        END))                       _30_day_unique_viewer_cnt ,
    COUNT(DISTINCT (src_fan_id))    to_date_unique_viewer_cnt,
    100001                       AS etl_batch_id,
    'bi_dbt_user_prd'            AS etl_insert_user_id,
    SYSDATE                      AS etl_insert_rec_dttm,
    ''                           AS etl_update_user_id,
    SYSDATE                      AS etl_update_rec_dttm
FROM
    {{source('fds_nplus','fact_daily_content_viewership')}} a
LEFT JOIN
    (
        SELECT
            production_id,
            episode_nm,
            debut_date,
            category_nm,
            event_style_nm,
            class_nm,
            series_group,
            content_tier,
            row_number() over (partition BY production_id ORDER BY as_on_date DESC) AS row
        FROM
            {{source('cdm','dim_content_classification_title')}}) b
ON
    a.production_id = b.production_id
AND row=1
WHERE
    b.category_nm='Event'
AND b.event_style_nm='PPV'
AND b.class_nm='In-Ring'
AND b.series_group='WWE PPV'
AND a.subs_tier=3
GROUP BY
    1,2,3,4,5,6,7,15,16,17,18,19