{{
  config({
	"schemas": 'fds_nplus',
	"materialized": 'view',
	"post-hook" : 'grant select on {{this}} to public'
	})
}}
--vw_rpt_nplus_weekly_ppv_lang_top_ctry_viewership
SELECT
    a.episode_nm,
    a.debut_date,
    a.view_date as as_on_date,
    a.tier,
    NVL(INITCAP(a.language),'Other') AS language,
    NVL(b.country_nm, 'Other')       AS country,
    NVL(b.region_nm, 'Other')        AS region,
    a.to_date_unique_viewer_cnt
FROM
    (
        SELECT
            episode_nm,
            language,
            tier,
            debut_date,
            view_date,
            CASE
                WHEN rank <5
                THEN country_cd
                ELSE 'Other'
            END                            AS country_cd,
            SUM(to_date_unique_viewer_cnt) AS to_date_unique_viewer_cnt
        FROM
            (
                SELECT
                    episode_nm,
                    language,
                    country_cd,
                    tier,
                    debut_date,
                    view_date,
                    to_date_unique_viewer_cnt,
                    row_number() over (partition BY episode_nm, language, view_date ORDER BY
                    to_date_unique_viewer_cnt DESC) AS rank
                FROM
                    {{ref('aggr_nplus_weekly_ppv_language_viewership')}})
        GROUP BY
            1,2,3,4,5,6) a
LEFT JOIN
    {{source('cdm','dim_region_country')}} b
ON
    LOWER(a.country_cd)=LOWER(b.country_nm)
AND ent_map_nm='GM Region'
AND src_sys_cd='iso'
where view_date = (select max(view_date) from {{ref('aggr_nplus_weekly_ppv_language_viewership')}})