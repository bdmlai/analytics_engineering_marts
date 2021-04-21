{{
  config({
		"materialized": 'ephemeral'
  })
}}


SELECT     n::int
FROM      (
                SELECT   row_number() over (ORDER BY true) as n
                FROM     {{source('fds_nl','rpt_nl_daily_minxmin_lite_log_ratings')}}
	  )  
CROSS JOIN            
	 (   
		SELECT MAX(regexp_count(talent, '[|]')) as max_num  
  		FROM   {{ref('intm_cp_nielsen_social_litelog_aggregate')}}
	) 
WHERE n <= max_num + 1
