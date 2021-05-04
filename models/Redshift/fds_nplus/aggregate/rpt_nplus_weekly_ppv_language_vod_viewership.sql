--weekly PPV Alternate Language Viewership by Country by time of the day
/*
***************************************************************************************************
**********************************************
Date        : 03/15/2021
Version     : 1.0
TableName   : rpt_nplus_weekly_ppv_language_vod_viewership
Schema    : fds_nplus
Contributor : Sandeep Battula
Description : This table aggregates the viewership data for Pay per view (PPV) Events based on the
time of the day, language and country. The metrics generated are Linear viewership, same day viewership, 3 day, 7
day and 30 day viewership
***************************************************************************************************
**********************************************
*/
--prehook - delete from fds_nplus.rpt_nplus_weekly_ppv_language_vod_viewership where as_on_date=
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
    (CURRENT_DATE - 1) AS as_on_date,
    CASE
        WHEN date_part('hour',stream_start_dttm) BETWEEN 0 AND 5
        THEN '12am-6am'
        WHEN date_part('hour',stream_start_dttm) BETWEEN 6 AND 9
        THEN '6am-10am'
        WHEN date_part('hour',stream_start_dttm) BETWEEN 10 AND 15
        THEN '10am-4pm'
        WHEN date_part('hour',stream_start_dttm) BETWEEN 16 AND 19
        THEN '4pm-8pm'
        WHEN date_part('hour',stream_start_dttm) >= 20
        THEN '8pm-12am'
    END AS time_slot,
    CASE
        WHEN DATEDIFF('day',debut_date,stream_start_dttm)=0
        THEN 'Debut'
        WHEN DATEDIFF('day',debut_date,stream_start_dttm)=1
        THEN 'Debut+1'
        WHEN DATEDIFF('day',debut_date,stream_start_dttm)=2
        THEN 'Debut+2'
        WHEN DATEDIFF('day',debut_date,stream_start_dttm)=3
        THEN 'Debut+3'
        WHEN DATEDIFF('day',debut_date,stream_start_dttm)=4
        THEN 'Debut+4'
        WHEN DATEDIFF('day',debut_date,stream_start_dttm)=5
        THEN 'Debut+5'
        WHEN DATEDIFF('day',debut_date,stream_start_dttm)=6
        THEN 'Debut+6'
        WHEN DATEDIFF('day',debut_date,stream_start_dttm)=7
        THEN 'Debut+7'
        ELSE 'Other'
    END                          AS view_day,
    min(a.dim_country_id) 		 AS dim_country_id,
	COUNT(DISTINCT (src_fan_id)) AS unique_viewer_cnt,
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
AND a.stream_type_cd = 'vod'
AND a.subs_tier=3
AND DATEDIFF('day',debut_date,stream_start_dttm) < 8
GROUP BY
    1,2,3,4,5,6,7,8,9,12,13,14,15,16