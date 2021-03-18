
select *,'DBT_'+TO_CHAR(convert_timezone('AMERICA/NEW_YORK', sysdate),'YYYY_MM_DD_HH_MI_SS')+'_CP' etl_batch_id, 
'bi_dbt_user_prd' AS etl_insert_user_id,
convert_timezone('AMERICA/NEW_YORK', sysdate) AS etl_insert_rec_dttm,
cast (NULL as varchar) AS etl_update_user_id,
CAST( NULL AS TIMESTAMP) AS etl_update_rec_dttm from #final order by platform, granularity, metric, year, week