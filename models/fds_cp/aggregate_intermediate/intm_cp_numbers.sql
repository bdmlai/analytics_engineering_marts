{{
  config({
		"materialized": 'ephemeral'
  })
}}

(select n::int from
  (select row_number() over (order by true) as n from {{source('fds_nl','rpt_nl_daily_minxmin_lite_log_ratings')}})
cross join
(select  max(regexp_count(talent, '[|]')) as max_num from {{ref('intm_cp_nielsen_social_litelog_aggregate')}} ) where n <= max_num + 1)