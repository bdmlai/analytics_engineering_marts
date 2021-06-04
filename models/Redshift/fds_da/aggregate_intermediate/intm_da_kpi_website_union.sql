{{
  config({
		"schema": 'fds_da',
		"materialized": 'ephemeral',"tags": 'rpt_da_monthly_wwe_com_website_kpis'
  })
}}


SELECT "t0"."date" AS "date",
  "t0"."device_category" AS "device_category",
  "t0"."device_type" AS "device_type",
  "t0"."geonetwork_us_v_international" AS "geonetwork_us_v_international",
  "t0"."page_type" AS "page_type",
  "t0"."landing_page_type" AS "landing_page_type",
  "t0"."hit_social_share_type" AS "hit_social_share_type",
  "t0"."trafficsource_channel" AS "trafficsource_channel",
  "t0"."hit_video_attempt" AS "hit_video_attempt",
  "t0"."metric_name" AS "metric_name",
  "t0"."metric_value" AS "metric_value",
  "t0"."etl_row_chksum_txt" AS "etl_row_chksum_txt",
  "t0"."etl_insert_rec_dttm" AS "etl_insert_rec_dttm",
  "t0"."etl_insert_user_id" AS "etl_insert_user_id",
  "t0"."etl_source_name" AS "etl_source_name",
  "t0"."etl_job_id" AS "etl_job_id",
  "t0"."hit_next" AS "hit_next",
  "t0"."hit_isexit" AS "hit_isexit",
  "t0"."hit_scroll" AS "hit_scroll",
  "t0"."scroll_number" AS "scroll_number",
  "t0"."pageview_count" AS "pageview_count",
  "t0"."table name" AS "table_name",
  case when ("t0"."table name"<>'dm_digital_kpi_datamart_monthly_website_scroll' and "metric_value"=0) then 'fail'  else 'pass' end as "metric_value_flag"

FROM (
  
  SELECT * FROM (
    SELECT "dm_digital_kpi_datamart_monthly_website"."date" AS "date",
      CAST("dm_digital_kpi_datamart_monthly_website"."device_category" AS TEXT) AS "device_category",
      CAST("dm_digital_kpi_datamart_monthly_website"."device_type" AS TEXT) AS "device_type",
      "dm_digital_kpi_datamart_monthly_website"."etl_insert_rec_dttm" AS "etl_insert_rec_dttm",
      CAST("dm_digital_kpi_datamart_monthly_website"."etl_insert_user_id" AS TEXT) AS "etl_insert_user_id",
      CAST("dm_digital_kpi_datamart_monthly_website"."etl_job_id" AS TEXT) AS "etl_job_id",
      CAST("dm_digital_kpi_datamart_monthly_website"."etl_row_chksum_txt" AS TEXT) AS "etl_row_chksum_txt",
      CAST("dm_digital_kpi_datamart_monthly_website"."etl_source_name" AS TEXT) AS "etl_source_name",
      CAST("dm_digital_kpi_datamart_monthly_website"."geonetwork_us_v_international" AS TEXT) AS "geonetwork_us_v_international",
      CAST(TRUNC(CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION)) AS BIGINT) AS "hit_isexit",
      CAST(TRUNC(CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION)) AS BIGINT) AS "hit_next",
      CAST(TRUNC(CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION)) AS BIGINT) AS "hit_scroll",
      CAST("dm_digital_kpi_datamart_monthly_website"."hit_social_share_type" AS TEXT) AS "hit_social_share_type",
      CAST("dm_digital_kpi_datamart_monthly_website"."hit_video_attempt" AS TEXT) AS "hit_video_attempt",
      CAST("dm_digital_kpi_datamart_monthly_website"."landing_page_type" AS TEXT) AS "landing_page_type",
      CAST("dm_digital_kpi_datamart_monthly_website"."metric_name" AS TEXT) AS "metric_name",
      "dm_digital_kpi_datamart_monthly_website"."metric_value" AS "metric_value",
      CAST("dm_digital_kpi_datamart_monthly_website"."page_type" AS TEXT) AS "page_type",
      CAST(TRUNC(CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION)) AS BIGINT) AS "pageview_count",
      CAST(NULL AS TEXT) AS "scroll_number",
      'dm_digital_kpi_datamart_monthly_website' AS "table name",
      CAST("dm_digital_kpi_datamart_monthly_website"."trafficsource_channel" AS TEXT) AS "trafficsource_channel"
    FROM  {{source('fds_da','dm_digital_kpi_datamart_monthly_website')}}
  ) "t1"
  
   UNION  ALL 
  
  SELECT * FROM (
    SELECT "dm_digital_kpi_datamart_monthly_website_scroll"."date" AS "date",
      CAST(NULL AS TEXT) AS "device_category",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."device_type" AS TEXT) AS "device_type",
      "dm_digital_kpi_datamart_monthly_website_scroll"."etl_insert_rec_dttm" AS "etl_insert_rec_dttm",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."etl_insert_user_id" AS TEXT) AS "etl_insert_user_id",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."etl_job_id" AS TEXT) AS "etl_job_id",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."etl_row_chksum_txt" AS TEXT) AS "etl_row_chksum_txt",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."etl_source_name" AS TEXT) AS "etl_source_name",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."geonetwork_us_v_international" AS TEXT) AS "geonetwork_us_v_international",
      "dm_digital_kpi_datamart_monthly_website_scroll"."hit_isexit" AS "hit_isexit",
      "dm_digital_kpi_datamart_monthly_website_scroll"."hit_next" AS "hit_next",
      "dm_digital_kpi_datamart_monthly_website_scroll"."hit_scroll" AS "hit_scroll",
      CAST(NULL AS TEXT) AS "hit_social_share_type",
      CAST(NULL AS TEXT) AS "hit_video_attempt",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."landing_page_type" AS TEXT) AS "landing_page_type",
      CAST(NULL AS TEXT) AS "metric_name",
      CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION) AS "metric_value",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."page_type" AS TEXT) AS "page_type",
      "dm_digital_kpi_datamart_monthly_website_scroll"."pageview_count" AS "pageview_count",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."scroll_number" AS TEXT) AS "scroll_number",
      'dm_digital_kpi_datamart_monthly_website_scroll' AS "table name",
      CAST("dm_digital_kpi_datamart_monthly_website_scroll"."trafficsource_channel" AS TEXT) AS "trafficsource_channel"
    FROM {{source('fds_da','dm_digital_kpi_datamart_monthly_website_scroll')}}
  ) "t2"
  
   UNION  ALL 
  
  SELECT * FROM (
    SELECT "dm_digital_kpi_datamart_monthly_website_uniques"."date" AS "date",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."device_category" AS TEXT) AS "device_category",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."device_type" AS TEXT) AS "device_type",
      "dm_digital_kpi_datamart_monthly_website_uniques"."etl_insert_rec_dttm" AS "etl_insert_rec_dttm",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."etl_insert_user_id" AS TEXT) AS "etl_insert_user_id",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."etl_job_id" AS TEXT) AS "etl_job_id",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."etl_row_chksum_txt" AS TEXT) AS "etl_row_chksum_txt",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."etl_source_name" AS TEXT) AS "etl_source_name",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."geonetwork_us_v_international" AS TEXT) AS "geonetwork_us_v_international",
      CAST(TRUNC(CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION)) AS BIGINT) AS "hit_isexit",
      CAST(TRUNC(CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION)) AS BIGINT) AS "hit_next",
      CAST(TRUNC(CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION)) AS BIGINT) AS "hit_scroll",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."hit_social_share_type" AS TEXT) AS "hit_social_share_type",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."hit_video_attempt" AS TEXT) AS "hit_video_attempt",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."landing_page_type" AS TEXT) AS "landing_page_type",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."metric_name" AS TEXT) AS "metric_name",
      "dm_digital_kpi_datamart_monthly_website_uniques"."metric_value" AS "metric_value",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."page_type" AS TEXT) AS "page_type",
      CAST(TRUNC(CAST(CAST(NULL AS TEXT) AS DOUBLE PRECISION)) AS BIGINT) AS "pageview_count",
      CAST(NULL AS TEXT) AS "scroll_number",
      'dm_digital_kpi_datamart_monthly_website_uniques' AS "table name",
      CAST("dm_digital_kpi_datamart_monthly_website_uniques"."trafficsource_channel" AS TEXT) AS "trafficsource_channel"
    FROM {{source('fds_da','dm_digital_kpi_datamart_monthly_website_uniques')}}
  ) "t3"
  
)"t0"


