{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'nxt_live_vod_kickoff' }) }}
SELECT
        a.*               ,
        b.prev_month_views,
        b.prev_year_views ,
        CASE WHEN a.platform = 'YouTube' THEN ROUND(a.views*0.23) ELSE 0 END AS us_views
FROM
        {{ ref("intm_nplus_live_plus_vod_manual_nxt") }} a
LEFT JOIN
        {{ ref("intm_nplus_prior_change_live_vod_nxt") }} b
ON
        a.platform    = b.platform
AND     a.event_brand = b.event_brand