{{
  config({
		'schema': 'fds_nplus',
		"materialized": 'view'
  })
}}
SELECT
    a.production_id,
    a.episode_nm,
    a.debut_date,
    a.view_date,
    a.tier,
    INITCAP(NVL(a.language,'Other')) AS language,
    NVL(b.country_nm, 'Other')       AS country,
    NVL(b.region_nm, 'Other')        AS region,
    a.live_unique_viewer_cnt,
    a.same_day_unique_viewer_cnt,
    a._3_day_unique_viewer_cnt,
    a._7_day_unique_viewer_cnt,
    a._30_day_unique_viewer_cnt,
    a.to_date_unique_viewer_cnt
FROM
    {{source('fds_nplus','aggr_nplus_daily_ppv_language_viewership')}} a
LEFT JOIN
    {{source('cdm','dim_region_country')}} b
ON
    LOWER(a.country_cd)=LOWER(b.country_nm)
AND ent_map_nm='GM Region'
AND src_sys_cd='iso'
where view_date = (select max(view_date) from fds_nplus.aggr_nplus_daily_ppv_language_viewership)