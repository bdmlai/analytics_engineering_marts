{{
  config({
		"materialized": 'ephemeral'
  })
}}


SELECT    a.show_date,
          a.show_name,
          storyline_1,
          comment,
          inpoint_est,
          outpoint_est,
          p2plusviewership_000 ,
          nielsen_twitter_interactions,
          b.p18_49viewership,
          talent
FROM      {{source('udl_cp','raw_storyline_daily_nielsen_social_litelog')}} a 
LEFT JOIN {{ref('intm_cp_minxmin_lite_log')}} b 
ON	a.show_Date=b.airdate 
	AND lower(a.show_name)=lower(b.title) 
	AND lower(a.segmenttype)=lower(b.segmenttype) 
	AND a.inpoint_est= b.modified_inpoint 
	AND a.outpoint_est = b.modified_outpoint 
WHERE storyline_1 IS NOT NULL