{{
  config({
		"schema": 'fds_nplus',
                "pre-hook": ["delete from fds_nplus.rpt_nplus_daily_milestone_complete_rates where premiere_date >= current_date -7"],
		"materialized": 'incremental','tags': "Content", "persist_docs": {'relation' : true, 'columns' : true}
  })
}}

select type,external_id,title,premiere_date,complete_rate,viewers_count,etl_batch_id,etl_insert_user_id,etl_insert_rec_dttm,etl_update_user_id,etl_update_rec_dttm
from 
(
select distinct 'PPV' as type, external_id, title, premiere_date, round(complete_rate,2):: float(2) complete_rate,
 count(src_fan_id) as viewers_count,
 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
  from
(select distinct *, 
case when show_sec <viewed_sec then 1.00 
     when show_sec = 0  then 0
else viewed_sec/show_sec end as complete_rate 
from
        (select distinct *, 
        content_duration as show_sec from
                (select distinct a.*, b.title, b.content_duration, b.premiere_date from 
                (select distinct external_id,src_fan_id,sum(case when time_spent<0 then 0 else time_spent end) as viewed_sec
                from {{ref('intm_nplus_viewership_data_with_externalid')}} 
				where external_id in (select external_id from {{ref('rpt_nplus_daily_ppv_streams')}})
				group by 1,2) a
                left join {{ref('intm_nplus_cg_ppv_streams_v2')}} b on a.external_id = b.external_id)
        )
)
group by 1,2,3,4,5 order by 1,2,3,4,5)

union all --live stream
select type,external_id,title,premiere_date,complete_rate,viewers_count,etl_batch_id,etl_insert_user_id,etl_insert_rec_dttm,etl_update_user_id,etl_update_rec_dttm
from 
(
select distinct '205 Live' as type, external_id, title, premiere_date, round(complete_rate,2):: float(2) complete_rate,
 count(src_fan_id) as viewers_count,
 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
  from
(select distinct *, 
case when show_sec <viewed_sec then 1.00 
     when show_sec = 0  then 0
else viewed_sec/cast (show_sec as int) end as complete_rate 
from
        (select distinct *, 
        content_duration as show_sec from
                (select distinct a.*, b.title, b.content_duration, b.airdate as premiere_date from 
                (select distinct external_id,src_fan_id,sum(case when time_spent<0 then 0 else time_spent end) as viewed_sec
                from {{ref('intm_nplus_viewershipdata_with_externalid_live')}}
                where external_id in (select external_id from {{ref('rpt_nplus_daily_live_streams')}})				
				group by 1,2) a
                left join {{ref('intm_nplus_cg_live_streams_v2')}} b on a.external_id = b.external_id)
        )
)
group by 1,2,3,4,5 order by 1,2,3,4,5)

union all --Nxt_tko stream
select type,external_id,title,premiere_date,complete_rate,viewers_count,etl_batch_id,etl_insert_user_id,etl_insert_rec_dttm,etl_update_user_id,etl_update_rec_dttm
from 
(
select distinct 'NXT' as type, external_id, title, premiere_date, round(complete_rate,2):: float(2) complete_rate,
 count(src_fan_id) as viewers_count,
 'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_content' as etl_batch_id,
 'bi_dbt_user_prd' as etl_insert_user_id, 
current_timestamp as etl_insert_rec_dttm, 
null as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
  from
(select distinct *, 
case when show_sec <viewed_sec then 1.00 
     when show_sec = 0  then 0
else viewed_sec/show_sec end as complete_rate 
from
        (select distinct *, 
        content_duration as show_sec from
                (select distinct a.*, b.title, b.content_duration, b.premiere_date from 
                (select distinct external_id,src_fan_id,sum(case when time_spent<0 then 0 else time_spent end) as viewed_sec
                from {{ref('intm_nplus_viewershipdata_with_externalid_nxt_tko')}} 
				where external_id in (select external_id from {{ref('rpt_nplus_daily_nxt_tko_streams')}})
				group by 1,2) a
                left join {{ref('intm_nplus_cg_nxt_tko_streams_v2')}} b on a.external_id = b.external_id)
        )
)
group by 1,2,3,4,5 order by 1,2,3,4,5)
