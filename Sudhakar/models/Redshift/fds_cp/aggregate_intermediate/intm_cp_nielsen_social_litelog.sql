{{
  config({
		"materialized": 'ephemeral'
  })
}}

select a.show_date,a.show_name,storyline_1,comment,inpoint_est,outpoint_est,p2plusviewership ,nielsen_twitter_interactions,
b.P18_49viewership,talent
 from {{source('udl_cp','raw_storyline_daily_nielsen_social_litelog')}} a left join
{{ref('intm_cp_minxmin_lite_log')}} b on
   a.show_Date=b.airdate and lower(a.show_name)=lower(b.title) and
lower(a.segmenttype)=lower(b.segmenttype) and 
a.inpoint_est= b.modified_inpoint and a.outpoint_est = b.modified_outpoint
where  storyline_1 is not null