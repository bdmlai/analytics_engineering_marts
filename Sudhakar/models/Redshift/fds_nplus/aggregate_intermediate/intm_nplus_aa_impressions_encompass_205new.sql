{{
  config({
		"materialized": 'ephemeral'
  })
}}

       
            SELECT CAST(CASE WHEN title= 'Live Event (Saudi Arabia Royal Rumble)' THEN to_timestamp(CAST('2018-04-27' as varchar) || ' ' 
			|| substring(inpoint, 1, 8), 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP + (12 * interval '1 hour')
            ELSE to_timestamp(CAST(airdate as varchar) || ' ' || substring(inpoint, 1, 8), 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP + 
			(18 * interval '1 hour') END as varchar) as start_time_join,
            CAST(CASE WHEN title= 'Live Event (Saudi Arabia Royal Rumble)' THEN '2018-04-27' ELSE airdate END as varchar) as start_date,
            substring(start_time_join, 12, 8) as start_time,
            CAST('LITE' as varchar) || CAST(logentrydbid as varchar) as content_id,
            comment as content_description,
            duration as duration,
            CAST(CASE WHEN title= 'Live Event (Saudi Arabia Royal Rumble)' THEN to_timestamp(CAST('2018-04-27' as varchar) || ' ' || substring(outpoint, 1, 8), 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP + (12 * interval '1 hour')
            ELSE to_timestamp(CAST(airdate as varchar) || ' ' || substring(outpoint, 1, 8), 'YYYY-MM-DD HH24:MI:SS')::TIMESTAMP + (18 * interval '1 hour') END as varchar) as end_time,
            CAST('Litelog' as varchar) as Ad_Abbreviation,
            CAST('Litelog ' as varchar) || CAST(segmenttype as varchar) as ad_category,
            CAST('Litelog' as varchar) as ad_type
            FROM {{source('udl_nplus','raw_post_event_log')}} a 
            left join {{source('udl_emm','emm_weekly_log_reference')}} b on a.logname = b.logname
            WHERE (airdate>='2018-09-19')
            AND (primary_us_platform in ('WWE','') OR primary_us_platform is null)
            AND segmenttype IN ('Sponsor Element','Promo','Lower Third','Promo Graphic')
            AND lower(title)='205 live'