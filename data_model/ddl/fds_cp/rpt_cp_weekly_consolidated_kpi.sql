/*
*******************************************************************************************
Date : 10/13/2020
Version : 1.0
ViewName : rpt_cp_weekly_consolidated_kpi
Schema : fds_cp
Contributor : Lakshman Murugeshan
Description : Reporting table is for consolidated KPI dashboard containing metrics related to Network subscriber and Digital platforms i.e. Youtube, Facebook, Twitter, Snapchat, Instagram and Doctom. This table is refreshed weekly as the metrics are aggregated weekly from Monday to Sunday.
***************************************************************************************************/
/* Date : 10/13/2020 ; Developer: Lakshman Murugeshan ; Change: Initial Version */

CREATE TABLE
    fds_cp.rpt_cp_weekly_consolidated_kpi
    (
        granularity CHARACTER VARYING(6) ENCODE LZO,
        platform CHARACTER VARYING(9) ENCODE LZO,
        type CHARACTER VARYING(9) ENCODE LZO,
        metric CHARACTER VARYING(43) ENCODE LZO,
        YEAR SMALLINT ENCODE AZ64,
        MONTH INTEGER ENCODE AZ64,
        week INTEGER ENCODE AZ64,
        start_date DATE ENCODE AZ64,
        end_date DATE ENCODE AZ64,
        value NUMERIC(38,1) ENCODE AZ64,
        prev_year SMALLINT ENCODE AZ64,
        prev_year_week INTEGER ENCODE AZ64,
        prev_year_start_date DATE ENCODE AZ64,
        prev_year_end_date DATE ENCODE AZ64,
        prev_year_value NUMERIC(38,1) ENCODE AZ64
    );
	
	
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.granularity   is 'represents the cadence i.e. Weekly or MTD or YTD'
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.platform     is 'represents the platform name i.e. Facebook, Network etc';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.type        is  'represents the youtube content types i.e. Owned or UGC';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.metric      is  'represents the metric name';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.year         is 'indicates the calendar year';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.month        is 'indicates the calendar month';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.week          is 'indicates the calendar week';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.start_date    is 'calander date representing the start date of the week';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.end_date      is 'calander date representing the end date of the week';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.value         is 'indicates the metric value for current year';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.prev_year     is 'indicates the previous calendar year';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.prev_year_week is 'indicates the week of previous calendar year';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.prev_year_start_date is 'calander date representing the previous year start date of the week';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.prev_year_end_date is 'calander date representing the previous year end date of the week';
	comment on column fds_cp.rpt_cp_weekly_consolidated_kpi.prev_year_value is 'indicates the metric value for previous year';