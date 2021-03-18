/*
************************************************************************************ 
    Date: 01/15/2021
    Version: 1.0
    Description: First Draft
    Contributor: Remya K Nair
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */
/* 01/15/2021 | Remya K Nair | Report table consists of total views for individual talends at video_id level- PSTA-1824 */


CREATE TABLE
    fds_yt.rpt_yt_daily_wwe_talent_views_pastyear
    (
        region_code CHARACTER VARYING(10) ENCODE ZSTD,
	region_name CHARACTER VARYING(256) ENCODE LZO,
        talent CHARACTER VARYING(65535) ENCODE LZO,  
        granularity CHARACTER VARYING(256) ENCODE LZO,     
        total_views BIGINT ENCODE ZSTD,  
        views_30days BIGINT ENCODE ZSTD,
        cnt_video_id  BIGINT ENCODE ZSTD,  		
        etl_batch_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64
        
    );
