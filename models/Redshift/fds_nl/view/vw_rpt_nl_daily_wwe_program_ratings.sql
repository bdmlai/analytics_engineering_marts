/*
*************************************************************************************************************************************************
   Date        : 07/21/2020
   Version     : 1.0
   TableName   : vw_rpt_nl_daily_wwe_live_program_ratings
   Schema	   : fds_nl
   Contributor : Rahul Chandran
   Description : WWE Program Ratings Daily Report View consist of rating details of all WWE Programs referencing from WWE Program Ratings Daily Report table
*************************************************************************************************************************************************
*/
{{
  config({
	'schema': 'fds_nl',"materialized": 'view','tags': "Phase4B","persist_docs": {'relation' : true, 'columns' : true},
	 'post_hook' : "grant select on {{ this }} to public"
	})
}}
select * from {{ref('rpt_nl_daily_wwe_program_ratings')}}