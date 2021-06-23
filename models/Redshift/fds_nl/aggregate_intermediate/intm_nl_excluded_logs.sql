{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT DISTINCT
    LOWER(trim(logname)) AS logname
FROM
    {{source('udl_emm','emm_weekly_log_reference')}}
WHERE
    LOWER(trim(primary_us_platform)) IN ('peacock',
                                         'wwe')