
CREATE VIEW
    VW_AGGR_NL_MONTHLY_HULU_WWE_VH_DATA comment =  '## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* ViewName    : vw_aggr_nl_monthly_hulu_wwe_vh_data
* Schema   : fds_nl
* Contributor : Hima Dasan
* Description : vw_aggr_nl_monthly_hulu_wwe_vh_data view consist of viewing hours of WWE Programs on monthly-basis in Hulu. 

## Maintenance Log
* Date : 06/19/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.'
    (
        flight_month COMMENT 'Month of program airing' ,
        flight_year COMMENT 'Year of program airing', 
        src_series_id COMMENT 'Program name the channel has broadcasted',
        tot_viewing_hours COMMENT 'Derived viewing hours'
    ) AS

select  
b.mth_abbr_nm as flight_month,
 date_part(YEAR,dateadd(day,adjusted_day_of_flight,flight_start_date)) as flight_year,
a.src_Series_id as src_Series_id,
sum((mc_us_aa_proj000*TOTAL_DURATION*1000.00)  /60.00 ) as tot_viewing_hours
 from   fds_nl.fact_nl_weekly_hulu_data a
 join cdm.dim_date  b on 
  date(dateadd(day,a.adjusted_day_of_flight,a.flight_start_date)) = b.full_date
group by 1,2,3 ;
