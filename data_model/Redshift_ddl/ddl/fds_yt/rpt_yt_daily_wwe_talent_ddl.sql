/*
************************************************************************************ 
    Date: 12/24/2020
    Version: 1.0
    Description: First Draft
    Contributor: Remya K Nair
    Adl' notes:  
    updated comments
**************************************************************************************/
/* date      | by      |    Details   */
/* 12/24/2020 | Remya K Nair | Report table consists of splitting talents present in talent pool - PSTA-1824 */


CREATE TABLE
    fds_yt.rpt_yt_daily_wwe_talent
    (
        report_date DATE ENCODE ZSTD,
        talent CHARACTER VARYING(65535) ENCODE LZO,
        video_id CHARACTER VARYING(100) ENCODE ZSTD,
        region_code CHARACTER VARYING(10) ENCODE ZSTD,
        total_views BIGINT ENCODE ZSTD,      		
        etl_batch_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_user_id CHARACTER VARYING(100) ENCODE LZO,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(50) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        
    );
