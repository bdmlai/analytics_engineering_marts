	-- Hulu wwe viewing hour aggregate view 

/*
*************************************************************************************************************************************************
   Date        : 06/19/2020
   Version     : 1.0
   TableName   : vw_aggr_nl_monthly_hulu_wwe_vh_data
   Schema	   : fds_nl
   Contributor : Hima Dasan
   Description : vw_aggr_nl_monthly_hulu_wwe_vh_data view consist of viewing hours of WWE Programs on monthly-basis in Hulu.
*************************************************************************************************************************************************
*/

{{
  config({
		"materialized": 'view','tags': "Phase4B","persist_docs": {'relation' : true, 'columns' : true}
  })
}}


select  
b.mth_abbr_nm as flight_month,
 datepart(YEAR,dateadd(day,adjusted_day_of_flight,flight_start_date)) as flight_year,
a.src_Series_id as src_Series_id,
sum((mc_us_aa_proj000*TOTAL_DURATION*1000.00)  /60.00 ) as tot_viewing_hours
 from   {{source('fds_nl','fact_nl_weekly_hulu_data')}} a
 join {{source('cdm','dim_date')}}  b on 
  date(dateadd(day,a.adjusted_day_of_flight,a.flight_start_date)) = b.full_date
group by 1,2,3


