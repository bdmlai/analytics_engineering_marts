

select airdate,title,segmenttype,modified_inpoint,modified_outpoint,
trim(concat(concat(nvl(talentactions,''),'|'),nvl(additionaltalent,''))) as talent_1 ,
case when left(talent_1,1) = '|' then substring(talent_1,2)
when right(talent_1,1) ='|' then substring(talent_1,1,length(talent_1)-1) else talent_1 end as talent,
sum(most_current_us_audience_avg_proj_000) as P18_49viewership
from "entdwdb"."fds_nl"."rpt_nl_daily_minxmin_lite_log_ratings"
 where src_demographic_group in ('Persons 18 - 49') and
  src_playback_period_cd = 'Live+SD | TV with Digital | Linear with VOD'
  group by 1,2,3,4,5,6