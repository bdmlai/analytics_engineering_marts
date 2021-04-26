{{
  config({
		'schema': 'fds_cp',
		"materialized": 'table','tags': "aggr_cp_daily_storyline_ui_data","persist_docs": {'relation' : true, 'columns' : true} ,
                "post-hook" : 'grant select on {{this}} to public'
  })
}}
SELECT show_date,
         show_name,
         story,
         talent,
         comments,
         appeared_est,
         average_viewers_p18_49,
         avg_p2_plus_viewership,
         agg_nielsen_tw_interactions,
         tw_interactions_ranking,
         'DBT_'+To_char(sysdate,'YYYY_MM_DD_HH_MI_SS')+'_CP' AS etl_batch_id, 'bi_dbt_user_prd' AS etl_insert_user_id, sysdate etl_insert_rec_dttm, '' etl_update_user_id, sysdate etl_update_rec_dttm
FROM 
    (SELECT a.show_date,
         a.show_name,
         a.storyline_1 AS story,
         Listagg(DISTINCT (Split_part(talent,
        '|',n)) ,'|') AS talent, comments, appeared AS appeared_est, average_viewers_p18_49, avg_p2_plus_viewership, agg_nielsen_tw_interactions, tw_interactions_ranking
    FROM {{ref('intm_cp_nielsen_social_litelog_aggregate')}} a
    CROSS JOIN {{ref('intm_cp_numbers')}}
    WHERE split_part(talent,'|',n) IS NOT NULL
            AND split_part(talent,'|',n) != ''
    GROUP BY  1,2,3,5,6,7,8,9,10 )