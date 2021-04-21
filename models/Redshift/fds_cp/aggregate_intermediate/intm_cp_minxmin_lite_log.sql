{{
  config({
		"materialized": 'ephemeral'
  })
}}

SELECT   airdate,
         title,
         segmenttype,
         modified_inpoint,
         modified_outpoint,
         Trim(Concat(Concat(nvl(talentactions,''),'|'),nvl(additionaltalent,''))) AS talent_1 ,
         CASE
                  WHEN LEFT(talent_1,1) = '|' THEN Substring(talent_1,2)
                  WHEN RIGHT(talent_1,1) ='|' THEN Substring(talent_1,1,Length(talent_1)-1)
                  ELSE talent_1
         END                                        AS talent,
         Sum(most_current_us_audience_avg_proj_000) AS p18_49viewership
FROM
{{source('fds_nl','rpt_nl_daily_minxmin_lite_log_ratings')}}
WHERE    src_demographic_group IN ('Persons 18 - 49') 
AND src_playback_period_cd = 'Live+SD | TV with Digital | Linear with VOD' 
GROUP BY 1,2,3,4,5,6