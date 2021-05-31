{{ config({ "schema": 'fds_nplus', "materialized": 'table',"tags": 'ppv_live_kickoff' }) }}
SELECT
        report_name     ,
        event           ,
        event_name      ,
        event_brand     ,
        series_name     ,
        event_date      ,
        start_timestamp ,
        end_timestamp   ,
        prev_month_event,
        prev_year_event ,
        platform        ,
        data_level      ,
        content_wwe_id  ,
        production_id   ,
        account         ,
        url             ,
        asset_id        ,
        CASE WHEN platform = 'Network' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        unique_viewers
                                FROM
                                        {{ ref("intm_nplus_live_nwk_unique_viewers_ppv") }}
                                WHERE
                                        event_brand = 'PPV') 
			 WHEN platform = 'WWE.COM' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        dotcom_plays
                                FROM
                                        {{ ref("intm_nplus_live_dotcom_plays_ppv") }}
                                WHERE
                                        event_brand = 'PPV') ELSE views 
		END AS views,
        minutes                                                                     ,
        prev_month_views                                                            ,
        prev_year_views                                                             ,
        CASE WHEN platform = 'Network' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        unique_viewers*0.75
                                FROM
                                        {{ ref("intm_nplus_live_nwk_unique_viewers_ppv") }}
                                WHERE
                                        event_brand = 'PPV') 
			 WHEN platform = 'WWE.COM' AND     event_brand = 'PPV' THEN
                        (
                                SELECT
                                        dotcom_us_plays
                                FROM
                                        {{ ref("intm_nplus_live_dotcom_plays_ppv") }}
                                WHERE
                                        event_brand = 'PPV') ELSE us_views 
		END AS us_views
FROM
        {{ ref("intm_nplus_live_manual_base_ppv") }}