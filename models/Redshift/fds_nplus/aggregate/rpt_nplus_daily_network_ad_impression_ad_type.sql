{{
  config({
		'schema': 'fds_nplus',
		"materialized": 'incremental',
		"unique_key": '(start_date||start_time||content_id||end_time||ad_abbreviation||ad_category||ad_type||audience_type)',
		"incremental_strategy": 'delete+insert',
		'tags': "rpt_nplus_daily_network_ad_impression_ad_type","persist_docs": {'relation' : true, 'columns' : true} ,
                "post-hook" : 'grant select on {{this}} to public'
  })
}}



SELECT
    start_date,
    start_time,
    content_id,
    content_description,
    duration,
    end_time,
    ad_abbreviation,
    ad_category,
    ad_type,
    audience_type,
    concurrent_plays,
    'DBT_'+TO_CHAR(SYSDATE,'YYYY_MM_DD_HH_MI_SS')+'_NPLUS' AS etl_batch_id,
    'bi_dbt_user_prd'                                      AS etl_insert_user_id,
    SYSDATE                                                   etl_insert_rec_dttm,
    ''                                                        etl_update_user_id,
    SYSDATE                                                   etl_update_rec_dttm
FROM
    (
        SELECT
            start_date,
            start_time,
            content_id,
            content_description,
            duration,
            end_time,
            ad_abbreviation,
            ad_category,
            ad_type,
            audience_type,
            CAST(REPLACE(concurrent_plays, ',', '') AS NUMERIC) AS concurrent_plays
        FROM
            (
                SELECT
                          t1.Start_Date,
                           t1. Start_Time,
                            t1.end_time,
                            t1.Duration,
                            t1.Content_ID,
                            t1.Content_Description,
                            t1.Ad_Abbreviation,
                            t1.ad_category,
                            t1.Ad_Type,
                    t2.filter_name AS audience_type,
                    t2.plays_upp   AS concurrent_plays
                FROM   {{ref('intm_nplus_aa_impressions_raweventlog_encompass')}} t1
                
                LEFT JOIN
                   {{ref('intm_nplus_aa_impressions_conviva_pulse')}} t2
                ON
                    t1.end_time> t2.ts2
                AND t1.end_time<=t2.ts2_upp
                     {% if is_incremental() %}
                           where start_date >= current_date- 7			
{% endif %}                     )
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11 )

