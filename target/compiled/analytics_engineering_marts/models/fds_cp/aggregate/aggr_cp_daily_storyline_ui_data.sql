


with __dbt__CTE__intm_cp_minxmin_lite_log as (


select airdate,title,segmenttype,modified_inpoint,modified_outpoint,
trim(concat(concat(nvl(talentactions,''),'|'),nvl(additionaltalent,''))) as talent_1 ,
case when left(talent_1,1) = '|' then substring(talent_1,2)
when right(talent_1,1) ='|' then substring(talent_1,1,length(talent_1)-1) else talent_1 end as talent,
sum(most_current_us_audience_avg_proj_000) as P18_49viewership
from "entdwdb"."fds_nl"."rpt_nl_daily_minxmin_lite_log_ratings"
 where src_demographic_group in ('Persons 18 - 49') and
  src_playback_period_cd = 'Live+SD | TV with Digital | Linear with VOD'
  group by 1,2,3,4,5,6
),  __dbt__CTE__intm_cp_nielsen_social_litelog as (


select a.show_date,a.show_name,storyline_1,comment,inpoint_est,outpoint_est,p2plusviewership_000,nielsen_twitter_interactions,
b.P18_49viewership,talent
 from "entdwdb"."udl_cp"."raw_storyline_daily_nielsen_social_litelog" a left join
__dbt__CTE__intm_cp_minxmin_lite_log b on
   a.show_Date=b.airdate and lower(a.show_name)=lower(b.title) and
lower(a.segmenttype)=lower(b.segmenttype) and 
a.inpoint_est= b.modified_inpoint and a.outpoint_est = b.modified_outpoint
where  storyline_1 is not null
),  __dbt__CTE__intm_cp_nielsen_social_litelog_aggregate as (


select  a.show_date,a.show_name,storyline_1,listagg(distinct a.comment, ';') as comments,
listagg(distinct (substring(a.inpoint_est,12,5)||' - '||substring(a.outpoint_est,12,5)),';') as appeared ,
listagg(distinct a.talent,'|') as talent,
(avg(P18_49viewership) / 
avg(c.average_total_p18_49) :: decimal(10,2)) as average_viewers_p18_49  ,
(avg(a.p2plusviewership_000) /
avg(c.avg_total_p2_plus) :: decimal(10,2)) as avg_p2_plus_viewership ,
SUM(nielsen_twitter_interactions) AS agg_nielsen_tw_interactions,
  row_number () over (partition by a.show_date,a.show_name
 order by agg_nielsen_tw_interactions desc) as tw_interactions_Ranking 
 from __dbt__CTE__intm_cp_nielsen_social_litelog a
left join 
(select  show_date,show_name,avg(p2plusviewership_000) as avg_total_p2_plus ,avg(P18_49viewership) as average_total_p18_49
 from __dbt__CTE__intm_cp_nielsen_social_litelog 
 group by 1,2) c
 on a.show_date=c.show_Date and lower(a.show_name)=lower(c.show_name) 
group by 1,2,3
),  __dbt__CTE__intm_cp_numbers as (


(select n::int from
  (select row_number() over (order by true) as n from "entdwdb"."fds_nl"."rpt_nl_daily_minxmin_lite_log_ratings")
cross join
(select  max(regexp_count(talent, '[|]')) as max_num from __dbt__CTE__intm_cp_nielsen_social_litelog_aggregate ) where n <= max_num + 1)
)select show_date,show_name,story,talent,comments,appeared,average_viewers_p18_49,avg_p2_plus_viewership,agg_nielsen_tw_interactions,
tw_interactions_Ranking,
'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 
		'bi_dbt_user_prd' as etl_insert_user_id,
		sysdate etl_insert_rec_dttm,
		'' etl_update_user_id,
		sysdate etl_update_rec_dttm 
		from (
select a.show_date,a.show_name,a.storyline_1 as story,listagg(distinct  (split_part(talent,'|',n)) ,'|') as talent,
 comments,appeared,average_viewers_p18_49,
avg_p2_plus_viewership,
agg_nielsen_tw_interactions,
tw_interactions_Ranking 
 from __dbt__CTE__intm_cp_nielsen_social_litelog_aggregate a
cross join __dbt__CTE__intm_cp_numbers
where split_part(talent,'|',n) is not null and split_part(talent,'|',n) != ''
group by 1,2,3,5,6,7,8,9,10
)