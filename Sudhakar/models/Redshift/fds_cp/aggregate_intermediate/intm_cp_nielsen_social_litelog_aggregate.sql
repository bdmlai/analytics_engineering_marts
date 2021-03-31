{{
  config({
		"materialized": 'ephemeral'
  })
}}

select  a.show_date,a.show_name,storyline_1,listagg(distinct a.comment, ';') as comments,
-- Changed the format for the appeared column from a 24HR format to a 12HR format , Jira : PSTA-2949
listagg(distinct substring(dateadd(hr,-12,a.inpoint_est),12,5) ||' - '||substring(dateadd(hr,-12,a.outpoint_est),12,5),';') as appeared ,
listagg(distinct a.talent,'|') as talent,
(avg(P18_49viewership) / 
avg(c.average_total_p18_49) :: decimal(10,2)) as average_viewers_p18_49  ,
(avg(a.p2plusviewership) /
avg(c.avg_total_p2_plus) :: decimal(10,2)) as avg_p2_plus_viewership ,
SUM(nielsen_twitter_interactions) AS agg_nielsen_tw_interactions,
  row_number () over (partition by a.show_date,a.show_name
 order by agg_nielsen_tw_interactions desc) as tw_interactions_Ranking 
 from {{ref('intm_cp_nielsen_social_litelog')}} a
left join 
(select  show_date,show_name,avg(p2plusviewership) as avg_total_p2_plus ,avg(P18_49viewership) as average_total_p18_49
 from {{ref('intm_cp_nielsen_social_litelog')}} 
 group by 1,2) c
 on a.show_date=c.show_Date and lower(a.show_name)=lower(c.show_name) 
group by 1,2,3