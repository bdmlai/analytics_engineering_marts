{{ config({ "schema": 'fds_nplus', "materialized": 'ephemeral',"tags": 'bump_liveshow' }) }}
SELECT
        a.*,
        b.prev_week_views
FROM
        {{ ref("intm_nplus_live_plus7_manual_bump") }} a
LEFT JOIN
        {{ ref("intm_nplus_prior_change_live_plus7_bump") }} b
ON
        a.platform    = b.platform
AND     a.event_brand = b.event_brand
AND     a.weekly_flag = b.weekly_flag