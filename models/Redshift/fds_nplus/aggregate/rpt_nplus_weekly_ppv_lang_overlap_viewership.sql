--Weekly PPV Alternate Language Overlap Viewership
/*
***************************************************************************************************
**********************************************
Date        : 04/01/2021
Version     : 1.0
TableName   : rpt_nplus_weekly_ppv_lang_overlap_viewership
Schema    : fds_nplus
Contributor : Sandeep Battula
Description : This table stores the overlap viewership data for unique viewers whoo viewed the content in english
and another Pay per view (PPV) Events based on the country. 
***************************************************************************************************
**********************************************
*/
{{
  config({
		"schema": 'fds_nplus',
		"materialized": 'table','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true},
		"post-hook": ["grant select on {{this}} to public"],
  })
}}
SELECT
    a.production_id,
    c.episode_nm,
    c.debut_date,
    a.language AS alternate_language,
    a.country_cd,
    CASE
        WHEN LOWER(c.episode_nm) LIKE '%wrestle%'
        OR  LOWER(c.episode_nm) LIKE '%summer%'
        OR  LOWER(c.episode_nm) LIKE '%survivor%'
        OR  (LOWER(c.episode_nm) LIKE '%royal%'
            AND LOWER(c.episode_nm) NOT LIKE '%greatest%')
        THEN 'Tier 1'
        ELSE 'Tier 2'
    END                           AS tier,
    (CURRENT_DATE-1)              AS as_on_date,
    MIN(a.dim_country_id)         AS dim_country_id,
    COUNT (DISTINCT b.src_fan_id) AS overlap_viewers,
    100001                        AS etl_batch_id,
    'bi_dbt_user_prd'             AS etl_insert_user_id,
    SYSDATE                       AS etl_insert_rec_dttm,
    ''                            AS etl_update_user_id,
    SYSDATE                       AS etl_update_rec_dttm
FROM
    {{source('fds_nplus','fact_daily_content_viewership')}} a
INNER JOIN
    {{source('fds_nplus','fact_daily_content_viewership')}} b
ON
    a.production_id = b.production_id
AND b.language='english'
AND a.src_fan_id=b.src_fan_id
AND a.country_cd=b.country_cd
LEFT JOIN
    (SELECT
            production_id,
            episode_nm,
            debut_date,
            category_nm,
            event_style_nm,
            class_nm,
            series_group,
            row_number() over (partition BY production_id ORDER BY as_on_date DESC) AS row
        FROM
            {{source('cdm','dim_content_classification_title')}}) c
ON
    a.production_id = c.production_id
AND row=1
WHERE
    a.subs_tier=3
AND a.event_style_nm='PPV'
AND b.subs_tier=3
AND b.event_style_nm='PPV'
AND c.event_style_nm='PPV'
AND c.category_nm='Event'
AND c.class_nm='In-Ring'
AND c.series_group='WWE PPV'
AND alternate_language<>'english'
GROUP BY
    1,2,3,4,5,6,7,10,11,12,13,14