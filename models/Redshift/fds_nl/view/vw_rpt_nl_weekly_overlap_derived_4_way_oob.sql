
/*
*************************************************************************************************************************************************
   Date        : 06/19/2020
   Version     : 1.0
   TableName   : vw_rpt_nl_weekly_overlap_derived_4_way_oob
   Schema	   : fds_nl
   Contributor : Remya K Nair
   Description : vw_rpt_nl_weekly_overlap_derived_4_way_oob view consists of weekly  overlap program schedules and unique reach for each time period 
*************************************************************************************************************************************************
*/

{{
  config({
		'schema': 'fds_nl',
		"materialized": 'view','tags': "Phase4B", "persist_docs": {'relation' : true, 'columns' : true}
  })
}}

select * from {{source('fds_nl','rpt_nl_weekly_overlap_derived_4_way_oob')}}  A 
 