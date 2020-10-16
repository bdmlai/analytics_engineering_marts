/*
*******************************************************************************************
Date : 10/13/2020
Version : 1.0
ViewName : rpt_cp_weekly_consolidated_kpi
Schema : fds_cp
Contributor : Lakshman Murugeshan
Description : Reporting table is for consolidated KPI dashboard containing metrics related to Network subscriber and Digital platforms i.e. Youtube, Facebook, Twitter, Snapchat, Instagram and Doctom. This table is refreshed weekly as the metrics are aggregated weekly from Monday to Sunday.
JIRA : PSTA-1077
***************************************************************************************************
Updates
TYPE JIRA DEVELOPER DATE DESCRIPTION
----- --------- ----- -----------
NEW PSTA-1077 Lakshman M 10/13/2020 Initial Version  */

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
	
	
