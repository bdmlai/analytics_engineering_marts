{{
  config({
		"materialized": 'ephemeral'
  })
}}

SELECT CAST(CASE
    WHEN title= 'Live Event (Saudi Arabia Royal Rumble)' THEN
    to_timestamp(CAST('2018-04-27' AS varchar) || ' ' || substring(inpoint, 1, 8), 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP + (12 * interval '1 hour')
    ELSE to_timestamp(CAST(airdate AS varchar) || ' ' || substring(inpoint, 1, 8), 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP + (18 * interval '1 hour')
    END AS varchar) AS start_time_join, 
    CAST(CASE
    WHEN title= 'Live Event (Saudi Arabia Royal Rumble)' THEN
    '2018-04-27'
    ELSE airdate
    END AS varchar) AS start_date, 
    substring(start_time_join, 12, 8) AS start_time,
    CAST('LITE' AS varchar) || CAST(logentrydbid AS varchar) AS content_id, 
    comment AS content_description,
    duration AS duration,
    CAST(CASE
    WHEN title= 'Live Event (Saudi Arabia Royal Rumble)' THEN
    to_timestamp(CAST('2018-04-27' AS varchar) || ' ' || substring(outpoint, 1, 8), 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP + (12 * interval '1 hour')
    ELSE to_timestamp(CAST(airdate AS varchar) || ' ' || substring(outpoint, 1, 8), 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP + (18 * interval '1 hour')
    END AS varchar) AS end_time, 
    CAST('Litelog' AS varchar) AS Ad_Abbreviation, 
    CAST('Litelog ' AS varchar) || CAST(segmenttype AS varchar) AS ad_category,
    CAST('Litelog' AS varchar) AS ad_type
FROM {{source('udl_nplus','raw_post_event_log')}} a
LEFT JOIN {{source('udl_emm','emm_weekly_log_reference')}} b
    ON a.logname = b.logname
WHERE (airdate>='2018-09-19')
        AND (primary_us_platform IN ('WWE','')
        OR primary_us_platform is null)
        AND segmenttype IN ('Sponsor Element','Promo','Lower Third','Promo Graphic')
        AND lower(title)='205 live'