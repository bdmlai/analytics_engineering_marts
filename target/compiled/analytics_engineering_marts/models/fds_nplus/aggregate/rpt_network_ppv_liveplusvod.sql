
select asset_id,production_id,event,event_name,event_date,start_time,end_time,platform,
views,us_views,minutes,per_us_views,prev_month_views,prev_month_event,prev_year_views,prev_year_event,monthly_per_change_views,
yearly_per_change_views,duration,overall_rank,yearly_rank,tier,monthly_color,yearly_color,
choose_ppv,event_brand,report_name,series_name,account,url,content_wwe_id as content_wweid,data_level,
'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', sysdate),'YYYY_MM_DD_HH_MI_SS')+'_PPV' etl_batch_id, 'bi_dbt_user_prd' AS etl_insert_user_id,
    convert_timezone('AMERICA/NEW_YORK', sysdate)                                   AS etl_insert_rec_dttm,
    NULL                                                AS etl_update_user_id,
    CAST( NULL AS TIMESTAMP)                            AS etl_update_rec_dttm
from #live_plusvod_final