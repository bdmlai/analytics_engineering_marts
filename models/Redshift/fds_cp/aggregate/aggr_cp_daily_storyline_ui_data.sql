{{
  config({
		'schema': 'fds_cp',
		"pre-hook": "truncate fds_cp.aggr_cp_daily_storyline_ui_data",
		"materialized": 'incremental','tags': "Content","persist_docs": {'relation' : true, 'columns' : true}
  })
}}


select show_date,show_name,story,talent,comments,appeared,average_viewers_p18_49,avg_p2_plus_viewership,agg_nielsen_tw_interactions,
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
 from {{ref('intm_cp_nielsen_social_litelog_aggregate')}} a
cross join {{ref('intm_cp_numbers')}}
where split_part(talent,'|',n) is not null and split_part(talent,'|',n) != ''
group by 1,2,3,5,6,7,8,9,10
)


 