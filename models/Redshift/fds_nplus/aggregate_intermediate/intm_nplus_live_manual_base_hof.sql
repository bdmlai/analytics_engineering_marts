{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'hof_live_kickoff' }) }}
SELECT
        a.*,
        CASE WHEN b.prev_month_event IS NULL THEN
                        (
                                SELECT DISTINCT
                                        prev_month_event
                                FROM
                                        {{ ref("intm_nplus_live_prior_change_hof") }}
                                WHERE
                                        prev_month_event IS NOT NULL) ELSE b.prev_month_event END AS prev_month_event,
        b.prev_month_views                                                                                           ,
        CASE WHEN b.prev_year_event IS NULL THEN
                        (
                                SELECT DISTINCT
                                        prev_year_event
                                FROM
                                        {{ ref("intm_nplus_live_prior_change_hof") }}
                                WHERE
                                        prev_year_event IS NOT NULL) ELSE b.prev_year_event END AS prev_year_event,
        b.prev_year_views                                                                                         ,
        CASE WHEN a.platform = 'YouTube' THEN ROUND(a.views*0.23) ELSE 0 END AS us_views
FROM
        {{ ref("intm_nplus_live_manual_hof") }} a
LEFT JOIN
        {{ ref("intm_nplus_live_prior_change_hof") }} b
ON
        a.platform = b.platform