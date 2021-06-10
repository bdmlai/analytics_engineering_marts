{{
  config({
		 'schema': 'fds_nplus',	
	     "materialized": 'view','tags': "bump_liveshow", "persist_docs": {'relation' : true, 'columns' : true},
		 "post-hook" : ['grant select on {{this}} to public']

        })
}}

select 
        event                  ,
        event_name             ,
        event_brand            ,
        report_name            ,
        series_name            ,
        event_date             ,
        start_time             ,
        end_time               ,
        platform               ,
        views                  ,
        minutes                ,
        peak_concurrents       ,
        prev_week_views        ,
        prev_week_event        ,
        weekly_per_change_views,
        duration               ,
        overall_rank           ,
        weekly_color           ,
        choose_event           ,
        production_id          ,
        content_wweid          ,
        data_level             ,
        weekly_flag            ,
        etl_batch_id           ,
        etl_insert_user_id     ,
        etl_insert_rec_dttm    ,
        etl_update_user_id     ,
        etl_update_rec_dttm

from {{ref('rpt_nplus_weekly_bump_live')}} 
--{{source('fds_nplus','rpt_nplus_weekly_bump_live')}}
