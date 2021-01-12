/*
*******************************************************************************************
Date : 11/13/2020
Version : 1.0
TableName : rpt_cp_daily_int_kpi_ranking_youtube
Schema : fds_cp
Contributor : Hima Dasan
Description : Reporting table consists of  daily international KPI ranking data for Youtube .Has metrics like views,netsubs,hours_watched,revenue,gross_revenue,population.
JIRA : PSTA-1538
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1538 Hima Dasan 11/13/2020 Initial Version  */

COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.country_code IS 'Indicates country code';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.year_month IS 'indicates year and month';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.content_type IS 'Indicates content type like WWE';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.recency IS 'Indicates recency whether recent or old';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.views IS 'Indicates the number of views';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.netsubs IS 'indicates network subscribers';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.hours_watched IS 'hours watched';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.revenue IS 'total revenue';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.gross_revenue IS 'The gross revenue';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.country_nm IS 'Indicates country name';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.region IS 'Indicates region name';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.population IS 'The total population of country';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.etl_batch_id IS 'Unique ID of DBT Job used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.etl_insert_user_id IS 'Unique ID of the DBT user that was used to insert the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.etl_insert_rec_dttm IS 'Date Time information on when the DBT inserted the record';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.etl_update_user_id IS 'Unique ID of the DBT user which was used to update the record manually';
COMMENT ON COLUMN fds_cp.rpt_cp_daily_int_kpi_ranking_youtube.etl_update_rec_dttm IS 'Date Time information on when the record was updated';    	

   