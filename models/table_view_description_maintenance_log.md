{% docs rpt_nl_daily_wwe_live_commercial_ratings %}

## Implementation Detail
* Date        : 06/12/2020
* Version     : 1.0
* TableName   : rpt_nl_daily_wwe_live_commercial_ratings
* Schema	  : fds_nl
* Contributor : Rahul Chandran
* Description : WWE Live Commercial Ratings Daily Report table consist of rating details of all WWE Live - RAW, SD, NXT Programs referencing from Commercial Viewership Ratings table on daily-basis

## Schedule Details
* Frequency : Weekly ; 04:00 A.M EST (Tue)
* Dependent Jobs (process_name ; process_id) : t_di_nielsen_fact_nl_commercial_viewership_ratings_abac ; 12132 (Tue) 

## Maintenance Log
* Date : 06/12/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
* Date : 09/21/2020 ; Developer: Remya K Nair   ; Change: Added columns program_telecast_rpt_starttime,program_telecast_rpt_endtime as a part of Phase 4b Project.
 
{% enddocs %}



{% docs rpt_nl_daily_wwe_live_quarterhour_ratings %}

## Implementation Detail
*   Date        : 06/12/2020
*   Version     : 1.0
*   TableName   : rpt_nl_daily_wwe_live_quarterhour_ratings
*   Schema	    : fds_nl
*   Contributor : Sudhakar Andugula
*   Description : WWE Live Quarter Hour Ratings Daily Report table consist of rating details of all WWE Live - RAW, SD, NXT Programs referencing from Quarter Hour Viewership Ratings Table on daily-basis 

## Schedule Details
* Frequency : Daily ; 02:00 A.M EST (Wed-Mon) & 04:00 A.M EST (Tue)
* Dependent Jobs (process_name ; process_id) : t_di_nielsen_fact_nl_quarterhour_viewership_ratings_daily_slot2_abac ; 12145 (Wed-Mon) & t_di_nielsen_fact_nl_quarterhour_viewership_ratings_abac ; 12122 (Tue)

## Maintenance Log
* Date : 06/12/2020 ; Developer: Sudhakar Andugula ; Change: Initial Version as a part of Phase 4b Project.

{% enddocs %}



{% docs rpt_nl_daily_wwe_program_ratings %}
## Implementation Detail
*   Date        : 07/21/2020
*   Version     : 1.0
*   TableName   : rpt_nl_daily_wwe_program_ratings
*   Schema	    : fds_nl
*   Contributor : Rahul Chandran
*   Description : WWE Program Ratings Daily Report View consist of rating details of all WWE Programs referencing from Program Viewership Daily fact table on daily-basis

## Schedule Details
* Frequency : Daily ; 02:00 A.M EST (Wed-Mon) & 04:00 A.M EST (Tue)
* Dependent Jobs (process_name ; process_id) : t_di_nielsen_fact_nl_program_viewership_ratings_daily_slot2_abac ; 12144 (Wed-Mon) & t_di_nielsen_fact_nl_program_viewership_ratings_abac ; 12121 (Tue)

## Maintenance Log
* Date : 07/21/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
* Date : 8/06/2020  ; Developer: Remya K Nair ; Enhancements program_rating report table -Exclude all the repeats while calculating the live viewership - TAB-2106 
* Date : 8/14/2020  ; Developer: Remya K Nair ; Enhancements program_rating report table - TAB-2105 
* Date : 12/07/2020 ; Developer: Remya K Nair ; Enhancement Added 11 columns to program_rating report table - PSTA-1349 
{% enddocs %}


{% docs rpt_nl_weekly_channel_switch %}

## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* TableName   : rpt_nl_weekly_channel_switch
* Schema	  : fds_nl
* Contributor : Hima Dasan
* Description : rpt_nl_weekly_channel_switch view consists of absolute stay ,absolute switch and ranking based on switch for WWE and AEW Programs 

## Schedule Details
* Frequency : Daily ; 12:00 A.M EST (Sun-Mon)
* Dependent Jobs (process_name ; process_id) : t_di_nielsen_fact_nl_weekly_live_switching_behavior_destination_dist_abac ; 12128,  t_di_nielsen_fact_nl_minxmin_ratings_aew_abac ; 12133, t_di_nielsen_fact_nl_minxmin_ratings_nxt_abac ; 12135, t_di_nielsen_fact_nl_minxmin_ratings_raw_abac ; 12136 and t_di_nielsen_fact_nl_minxmin_ratings_smackdown_abac ; 12137 (Sun-Mon)

## Maintenance Log
* Date : 06/19/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
* Date : 08/28/2020 ; Developer: Hima Dasan ; Change: Enhancement to remove commercial break minutes and starting and ending 5 minutes from ranking.

{% enddocs %}



{% docs vw_rpt_nl_weekly_channel_switch %}

## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* TableName   : vw_rpt_nl_weekly_channel_switch
* Schema	  : fds_nl
* Contributor : Hima Dasan
* Description : vw_rpt_nl_weekly_channel_switch view consists of absolute stay ,absolute switch and ranking based on switch for WWE, AEW and other wrestling programs (Weekly)

## Maintenance Log
* Date : 06/19/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.


{% enddocs %}



{% docs vw_aggr_nl_monthly_hulu_wwe_vh_data %}

## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* ViewName    : vw_aggr_nl_monthly_hulu_wwe_vh_data
* Schema	  : fds_nl
* Contributor : Hima Dasan
* Description : vw_aggr_nl_monthly_hulu_wwe_vh_data view consist of viewing hours of WWE Programs on monthly-basis in Hulu. 

## Maintenance Log
* Date : 06/19/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.


{% enddocs %}

{% docs vw_aggr_nl_monthly_timeperiod_ratings %}

## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* ViewName    : vw_aggr_nl_monthly_timeperiod_ratings
* Schema	  : fds_nl
* Contributor : Remya K Nair
* Description :vw_aggr_nl_monthly_timeperiod_ratings View consist of rating details of all channels and programs to be rolled up from Timeperiod Viewership Ratings table on monthly-basis. 

## Maintenance Log
* Date : 06/19/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.


{% enddocs %}


{% docs vw_aggr_nl_monthly_wwe_live_program_ratings %}

## Implementation Detail
* Date        : 07/30/2020
* Version     : 1.0
* ViewName    : vw_aggr_nl_monthly_wwe_live_program_ratings
* Schema	  : fds_nl
* Contributor : Remya K Nair
* Description :WWE Live Program Rating Monthly Aggregate View consist of rating details of all WWE Live - RAW, SD, NXT Programs to be rolled up from WWE Program Ratings Daily Report Table on monthly-basis


## Maintenance Log
* Date : 07/30/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.


{% enddocs %}


{% docs vw_rpt_nl_weekly_overlap_chart %}

## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* ViewName    : vw_rpt_nl_weekly_overlap_chart
* Schema	  : fds_nl
* Contributor : Remya K Nair
* Description :vw_rpt_nl_weekly_overlap_chart view consists of Derived Columns for Overlap data for  WWE, AEW and other wrestling programs 

## Maintenance Log
* Date : 06/19/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.


{% enddocs %}


{% docs rpt_nl_weekly_overlap_program_4_way_oob %}

## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* ViewName    : rpt_nl_weekly_overlap_program_4_way_oob
* Schema	  : fds_nl
* Contributor : Remya K Nair
* Description :vw_rpt_nl_weekly_overlap_program_4_way_oob view consists of  Schedule (Both Staright Neilsen Run and Derived) details  and calculations for Overlap data for  WWE, AEW and other wrestling programs 

## Schedule Details
* Frequency : Daily ; 10:00 P.M EST 
* Dependent Jobs (process_name ; process_id) : t_di_nielsen_fact_nl_wkly_overlap_4_way_oob_abac ; 12126  

## Maintenance Log
* Date : 06/19/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.


{% enddocs %}



{% docs vw_rpt_nl_weekly_overlap_program_4_way_oob %}

## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* ViewName    : vw_rpt_nl_weekly_overlap_program_4_way_oob
* Schema	  : fds_nl
* Contributor : Remya K Nair
* Description : vw_rpt_nl_weekly_overlap_program_4_way_oob view consists of  Schedule (Both Staright Neilsen Run and Derived) details  and calculations for Overlap data for  WWE, AEW and other wrestling programs
 
## Maintenance Log
* Date : 06/19/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.


{% enddocs %}


{% docs vw_aggr_nplus_ppv_week_adds_tracking %}
## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* ViewName    : vw_aggr_nplus_ppv_week_adds_tracking
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : vw_aggr_nplus_ppv_week_adds_tracking view consists of  PPV event details with count of paid/trail/promo subscription add and forecast suscription count
## Maintenance Log
* Date : 06/19/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.
{% enddocs %}


{% docs vw_rpt_network_daily_subscription_kpis %}
## Implementation Detail
* Date        : 06/29/2020
* Version     : 1.0
* ViewName    : vw_rpt_network_daily_subscription_kpis
* Schema	  : fds_nl
* Contributor : Lakshman Murugeshan
* Description : vw_rpt_network_daily_subscription_kpis view consists of network daily kpi information
## Maintenance Log
* Date : 06/29/2020 ; Developer: Lakshman Murugeshan DBT: Sudhakar ; Change: Initial Version as a part of network dashboards.
{% enddocs %}


{% docs vw_rpt_network_ppv_actuals_estimates_forecast %}
## Implementation Detail
* Date        : 06/21/2020
* Version     : 1.0
* ViewName    : vw_rpt_network_ppv_actuals_estimates_forecast
* Schema	  : fds_nl
* Contributor : Sudhakar Andugula
* Description : vw_rpt_network_ppv_actuals_estimates_forecast view consists of network daily kpi information
## Maintenance Log
* Date : 06/21/2020 ; Developer: Sudhakar Andugula DBT: Sudhakar ; Change: Initial Version as a part of network dashboards.
{% enddocs %}


{% docs aggr_cp_weekly_consumption_by_platform %}
## Implementation Detail
* Date        : 07/09/2020
* Version     : 2.0
* ViewName    : aggr_cp_weekly_consumption_by_platform
* Schema	  : fds_cp
* Contributor : Sandeep Battula
* Description : aggr_cp_weekly_consumption_by_platform This aggregate table stores the crossplatform consumption metrics - total views and total minutes watched aggregated for each week for platforms- Youtube, facebook, Twitter, Instagram, Snapchat and dotcom/App.
## Maintenance Log
* Date : 06/21/2020 ; Developer: Code: Sandeep Battula, DBT: Sudhakar ; Change: Initial Version as a part of network dashboards.
* Date : 08/24/2020 ; Developer: Code: Sandeep Battula, DBT: Sudhakar ; Change: As a part weekly cross platform consumption enhancements to add Youtube-UGC, 	WWE Network and TikTok platform

{% enddocs %}



{% docs vw_aggr_cp_weekly_consumption_by_platform %}
## Implementation Detail
* Date        : 07/09/2020
* Version     : 1.0
* ViewName    : vw_aggr_cp_weekly_consumption_by_platform
* Schema	  : fds_cp
* Contributor : Sandeep Battula
* Description : vw_aggr_cp_weekly_consumption_by_platform This aggregate table stores the crossplatform consumption metrics - total views and total minutes watched aggregated for each week for platforms- Youtube, facebook, Twitter, Instagram, Snapchat and dotcom/App.
## Maintenance Log
* Date : 06/21/2020 ; Developer: Code: Sandeep Battula, DBT: Sudhakar ; Change: Initial Version as a part of network dashboards.
{% enddocs %}



{% docs vw_rpt_weekly_network_subscriber_kpis_ir %}
## Implementation Detail
* Date        : 07/09/2020
* Version     : 1.0
* ViewName    : vw_rpt_weekly_network_subscriber_kpis_ir
* Schema	  : fds_cp
* Contributor : Sandeep Battula
* Description : vw_rpt_weekly_network_subscriber_kpis_ir Weekly Network Subscriber KPIs for IR Team
## Maintenance Log
* Date : 06/21/2020 ; Developer: Code: Sandeep Battula, DBT: Sudhakar ; Change: Initial Version as a part of network dashboards.
{% enddocs %}


{% docs vw_aggr_daily_network_adds_and_loss_track %}
## Implementation Detail
* Date        : 07/15/2020
* Version     : 1.0
* ViewName    : vw_aggr_daily_network_adds_and_loss_track
* Schema	  : fds_nplus
* Contributor : Sudhakar Andugula
* Description : vw_aggr_daily_network_adds_and_loss_track view cosists of details about total subscription losses of orders
## Maintenance Log
* Date : 07/15/2020 ; Developer: Sudhakar Andugula ; Change: Initial Version as a part of Network 2.0 Project.
{% enddocs %}


{% docs vw_aggr_kntr_monthly_wwe_program_rating_schedule %}
## Implementation Detail
* Date        : 07/26/2020
* Version     : 1.0
* ViewName    : vw_aggr_kntr_monthly_wwe_program_rating_schedule
* Schema	  : fds_kntr
* Contributor : Remya K Nair
* Description : vw_aggr_kntr_monthly_wwe_program_rating_schedule view  consist of  Monthly RAW,SD,NXT and PPVs ratings for Live & Nth runs on monthly-basis
## Maintenance Log
* Date : 07/26/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}


{% docs vw_aggr_kntr_quarterly_wwe_program_rating_schedule %}
## Implementation Detail
* Date        : 07/26/2020
* Version     : 1.0
* ViewName    : vw_aggr_kntr_quarterly_wwe_program_rating_schedule
* Schema	  : fds_kntr
* Contributor : Remya K Nair
* Description : vw_aggr_kntr_quarterly_wwe_program_rating_schedule view  consist of  quarterly RAW,SD,NXT and PPVs ratings for Live & Nth runs on quarterly-basis
## Maintenance Log
* Date : 07/26/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}


{% docs vw_aggr_kntr_yearly_wwe_program_rating_schedule %}
## Implementation Detail
* Date        : 07/26/2020
* Version     : 1.0
* ViewName    : vw_aggr_kntr_yearly_wwe_program_rating_schedule
* Schema	  : fds_kntr
* Contributor : Remya K Nair
* Description : vw_aggr_kntr_yearly_wwe_program_rating_schedule view  consist of  yearly RAW,SD,NXT and PPVs ratings for Live & Nth runs on yearly-basis
## Maintenance Log
* Date : 07/26/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}



{% docs vw_aggr_kntr_weekly_wwe_program_rating %}
## Implementation Detail
* Date        : 07/17/2020
* Version     : 1.0
* ViewName    : vw_aggr_kntr_weekly_wwe_program_rating
* Schema      : fds_kntr
* Contributor : Hima Dasan
* Description : WWE  rogram Rating Weekly  Aggregate View consist of rating details of all WWE  Programs to be rolled up from wwe telecast data on weekly-basis
## Maintenance Log
* Date : 07/17/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}



{% docs vw_aggr_kntr_monthly_wwe_program_rating %}
## Implementation Detail
* Date        : 07/17/2020
* Version     : 1.0
* ViewName    : vw_aggr_kntr_monthly_wwe_program_rating
* Schema      : fds_kntr
* Contributor : Hima Dasan
* Description : WWE  rogram Rating monthly  Aggregate View consist of rating details of all WWE  Programs to be rolled up from wwe telecast data on monthly-basis
## Maintenance Log
* Date : 07/17/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}


{% docs vw_aggr_kntr_quarterly_wwe_program_rating %}
## Implementation Detail
* Date        : 07/17/2020
* Version     : 1.0
* ViewName    : vw_aggr_kntr_quarterly_wwe_program_rating
* Schema      : fds_kntr
* Contributor : Hima Dasan
* Description : WWE  rogram Rating quarterly  Aggregate View consist of rating details of all WWE  Programs to be rolled up from wwe telecast data on quaterly-basis
## Maintenance Log
* Date : 07/17/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}


{% docs vw_aggr_kntr_yearly_wwe_program_rating %}
## Implementation Detail
* Date        : 07/17/2020
* Version     : 1.0
* ViewName    : vw_aggr_kntr_yearly_wwe_program_rating
* Schema      : fds_kntr
* Contributor : Hima Dasan
* Description : WWE  rogram Rating yearly  Aggregate View consist of rating details of all WWE  Programs to be rolled up from wwe telecast data on yearly-basis
## Maintenance Log
* Date : 07/17/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs aggr_kntr_weekly_competitive_program_ratings %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   TableName   : aggr_kntr_weekly_competitive_program_ratings
*   Schema	    : fds_kntr
*   Contributor : Rahul Chandran
*   Description : Competitive Program Ratings Weekly Aggregate Table consist of rating details of competitive programs referencing from Annual Profile Table on weekly-basis

## Maintenance Log
* Date : 07/24/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs vw_aggr_kntr_weekly_competitive_program_ratings %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_kntr_weekly_competitive_program_ratings
*   Schema	    : fds_kntr
*   Contributor : Rahul Chandran
*   Description : Competitive Program Ratings Weekly Aggregate View consist of rating details of competitive programs referencing from Competitive Weekly Program Ratings Table on weekly-basis

## Maintenance Log
* Date : 07/24/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs vw_aggr_kntr_monthly_competitive_program_ratings %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_kntr_monthly_competitive_program_ratings
*   Schema	    : fds_kntr
*   Contributor : Rahul Chandran
*   Description : Competitive Program Ratings Monthly Aggregate View consist of rating details of competitive programs referencing from Competitive Weekly Program Ratings Table on monthly-basis

## Maintenance Log
* Date : 07/24/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs vw_aggr_kntr_quarterly_competitive_program_ratings %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_kntr_quarterly_competitive_program_ratings
*   Schema	    : fds_kntr
*   Contributor : Rahul Chandran
*   Description : Competitive Program Ratings Quarterly Aggregate View consist of rating details of competitive programs referencing from Competitive Weekly Program Ratings Table on quarterly-basis

## Maintenance Log
* Date : 07/24/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs vw_aggr_kntr_yearly_competitive_program_ratings %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_kntr_yearly_competitive_program_ratings
*   Schema	    : fds_kntr
*   Contributor : Rahul Chandran
*   Description : Competitive Program Ratings Yearly Aggregate View consist of rating details of competitive programs referencing from Competitive Weekly Program Ratings Table on yearly-basis

## Maintenance Log
* Date : 07/24/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}


{% docs rpt_kntr_schedule_vh_data %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   TableName   : rpt_kntr_schedule_vh_data
*   Schema	    : fds_kntr
*   Contributor : Rahul Chandran
*   Description : WWE Program Schedule Viewing Hours Report table consist of rating and other details of WWE program schedule referencing from WWE Telecast Data table on daily-basis

## Maintenance Log
* Date : 07/24/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs vw_rpt_kntr_schedule_vh_data %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_kntr_schedule_vh_data
*   Schema	    : fds_kntr
*   Contributor : Rahul Chandran
*   Description : WWE Program Schedule Viewing Hours Report View consist of rating and other details of WWE program schedule referencing from Schedule VH Data Report Table on daily-basis

## Maintenance Log
* Date : 07/24/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs vw_aggr_kntr_schedule_wca_data %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_kntr_schedule_wca_data
*   Schema	    : fds_kntr
*   Contributor : Hima Dasan
*   Description : WWE Program Schedule Weekly Cumulative Audience Aggregate View consist of rating and other details of WWE program schedule referencing from Schedule VH Data Report Table on weekly-basis

## Maintenance Log
* Date : 07/24/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs vw_aggr_kntr_monthly_country_vh %}
## Implementation Detail
*   Date        : 07/24/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_kntr_monthly_country_vh
*   Schema	    : fds_kntr
*   Contributor : Hima Dasan
*   Description : View calculates actual viewing hour on monthly basis and calculates estimate value for last month for all WWE programs

## Maintenance Log
* Date : 07/24/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
* Date : 09/22/2020 ; Developer: Rahul Chandran ; Change: Enhancement has done as requested as per Jira Request: PSTA-1153.
{% enddocs %}


{% docs aggr_monthly_network_kpis_vkm %}
## Implementation Detail
*   Date        : 07/28/2020
*   Version     : 1.0
*   ViewName    : aggr_monthly_network_kpis_vkm
*   Schema	: fds_nplus
*   Contributor : Lakshman Murugeshan
*   Description : monthly VKM network kpis

## Maintenance Log
* Date : 07/28/2020 ; Developer: Lakshman Murugeshan ; DBT : Sudhakar Change: Initial Version
{% enddocs %}


{% docs vw_aggr_monthly_network_kpis_vkm %}
## Implementation Detail
*   Date        : 07/28/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_monthly_network_kpis_vkm
*   Schema	: fds_nplus
*   Contributor : Lakshman Murugeshan
*   Description : monthly VKM network kpis

## Maintenance Log
* Date : 07/28/2020 ; Developer: Lakshman Murugeshan ; DBT : Sudhakar Change: Initial Version
{% enddocs %}


{% docs rpt_network_ppv_liveplusvod %}
## Implementation Detail
*   Date        : 07/28/2020
*   Version     : 1.0
*   ViewName    : rpt_network_ppv_liveplusvod
*   Schema	: fds_nplus
*   Contributor : Lakshman Murugeshan
*   Description : View contains the information related to Live NXT and HOF evenet

## Maintenance Log
* Date : 07/28/2020 ; Developer: Lakshman Murugeshan ; DBT & Python Automation: Sudhakar; Change: Initial Version
{% enddocs %}


{% docs vw_rpt_network_ppv_liveplusvod %}
## Implementation Detail
*   Date        : 07/28/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_network_ppv_liveplusvod
*   Schema	: fds_nplus
*   Contributor : Lakshman Murugeshan
*   Description : View contains the information related to Live NXT and HOF evenet

## Maintenance Log
* Date : 07/28/2020 ; Developer: Lakshman Murugeshan ; DBT & Python Automation: Sudhakar; Change: Initial Version
{% enddocs %}


{% docs rpt_cp_monthly_global_consumption_by_platform %}
## Implementation Detail
*   Date        : 07/14/2020
*   Version     : 1.0
*   ViewName    : rpt_cp_monthly_global_consumption_by_platform
*   Schema	: fds_cp
*   Contributor : Sandeep Battula
*   Description : Monthly Cross Platform Global Content Consumption aggregate table consists of consumption metrics Views and Hours watched with country and 	region details for all cross platforms. This script inserts last month data for platforms- Youtube, Facebook, WWE.Com and WWE App, Instagram, Snapchat and Twitter from respective source tables on monthly basis (5th of every month). Inaddition to the latest month, metrics are also calculated and inserted for previous month, year-to-date and previous year-to-date. 
*   Date        : 07/14/2020
*   Change      : Enhancement is done for Domestic TV platform to get viewing hours for Live+Same day for the dates Live+7 data is not available.  Also updated prehook to delete current month's data for platforms - Domestic TV, PPTV, Hulu SVOD and WWE Network inaddition to current International TV

## Maintenance Log
* Date : 07/14/2020 ; Developer: Sandeep Battula ; DBT & Python Automation: Sudhakar; Change: Initial Version
{% enddocs %}


{% docs vw_rpt_cp_monthly_global_consumption_by_platform %}
## Implementation Detail
*   Date        : 07/14/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cp_monthly_global_consumption_by_platform
*   Schema	: fds_cp
*   Contributor : Sandeep Battula
*   Description : Monthly Cross Platform Global Content Consumption aggregate table consists of consumption metrics Views and Hours watched with country and 	region details for all cross platforms. This script inserts last month data for platforms- Youtube, Facebook, WWE.Com and WWE App, Instagram, Snapchat and Twitter from respective source tables on monthly basis (5th of every month). Inaddition to the latest month, metrics are also calculated and inserted for previous month, year-to-date and previous year-to-date. 

## Maintenance Log
* Date : 07/14/2020 ; Developer: Sandeep Battula ; DBT & Python Automation: Sudhakar; Change: Initial Version
{% enddocs %}



{% docs vw_rpt_nplus_monthly_marketing_subs %}
## Implementation Detail
*   Date        : 08/14/2020
*   Version     : 1.0
*   TableName   : vw_rpt_nplus_monthly_marketing_subs
*   Schema      : fds_nplus
*   Contributor : Hima Dasan
*   Description : vw_rpt_nplus_monthly_marketing_subs view consist of Actuals,forecast and Budget for adds and Disconnects For Roku,Apple and mlbam (Monthly)

## Maintenance Log
* Date : 08/14/2020 ; Developer: Hima Dasan; Change: Initial Version
{% enddocs %}

{% docs rpt_nl_daily_minxmin_lite_log_ratings %}
## Implementation Detail
*   Date        : 08/17/2020
*   Version     : 1.0
*   TableName   : rpt_nl_daily_minxmin_lite_log_ratings
*   Schema	    : fds_nl
*   Contributor : Rahul Chandran
*   Description : Minute By Minture Ratings joining with Lite Log Report table consist of ratings of segments along with its details referencing from Minute By Minute Ratings and Lite Log tables on daily-basis

## Maintenance Log
* Date : 08/17/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs vw_rpt_nl_daily_minxmin_lite_log_ratings %}
## Implementation Detail
*   Date        : 08/17/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_nl_daily_minxmin_lite_log_ratings
*   Schema	    : fds_nl
*   Contributor : Rahul Chandran
*   Description : Minute By Minture Ratings joining with Lite Log Report View consist of ratings of segments along with its details referencing from Minute By Minute Ratings joining with Lite Log daily table

## Maintenance Log
* Date : 08/17/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 4b Project.
{% enddocs %}

{% docs aggr_cpg_daily_sales %}
## Implementation Detail
*   Date        : 08/28/2020
*   Version     : 1.0
*   ViewName    : aggr_cpg_daily_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Aggregated CPG Daily Sales Table consist of Sales details of WWE products on daily-basis

## Schedule Details
* Frequency : Daily ; 08:00 A.M EST 
* Dependent Jobs (process_name ; process_id) : t_di_cpg_fact_cpg_sales_detail_radial_abac ; 30230,            t_di_cpg_fact_cpg_sales_detail_cb_abac ; 30231, t_di_cpg_fact_cpg_sales_detail_amazon_abac ; 30232, t_di_cpg_fact_cpg_sales_detail_kit_component_radial_abac ; 30233, t_di_cpg_fact_cpg_sales_header_radial_abac ; 30234, t_di_cpg_fact_cpg_sales_header_cb_abac ; 30235 & t_di_cpg_fact_cpg_sales_header_amazon_abac ; 30236

## Maintenance Log
* Date : 08/28/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5b Project.
{% enddocs %}

{% docs vw_aggr_cpg_daily_sales %}
## Implementation Detail
*   Date        : 08/28/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_cpg_daily_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Aggregated CPG Daily Sales Table View of Sales details of WWE products on daily-basis

## Maintenance Log
* Date : 08/28/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5b Project.
{% enddocs %}

{% docs aggr_cpg_daily_kit_sales %}
## Implementation Detail
*   Date        : 08/28/2020
*   Version     : 1.0
*   ViewName    : aggr_cpg_daily_kit_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Aggregated CPG Daily Kit Sales Table consist of Sales details of WWE Kit products on daily-basis

## Schedule Details
* Frequency : Daily ; 08:00 A.M EST 
* Dependent Jobs (process_name ; process_id) : t_di_cpg_fact_cpg_sales_detail_radial_abac ; 30230,            t_di_cpg_fact_cpg_sales_detail_cb_abac ; 30231, t_di_cpg_fact_cpg_sales_detail_amazon_abac ; 30232,      t_di_cpg_fact_cpg_sales_header_radial_abac ; 30234, t_di_cpg_fact_cpg_sales_header_cb_abac ; 30235 &     t_di_cpg_fact_cpg_sales_header_amazon_abac ; 30236

## Maintenance Log
* Date : 08/28/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5b Project.
{% enddocs %}

{% docs vw_aggr_cpg_daily_kit_sales %}
## Implementation Detail
*   Date        : 08/28/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_cpg_daily_kit_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Aggregated CPG Daily Kit Sales View consist of Sales details of WWE Kit products on daily-basis

## Maintenance Log
* Date : 08/28/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5b Project.
{% enddocs %}

{% docs aggr_cpg_daily_venue_sales %}
## Implementation Detail
*   Date        : 09/23/2020
*   Version     : 1.0
*   ViewName    : aggr_cpg_daily_venue_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Aggregated CPG Daily Venue Sales Table consist of Sales details of WWE products on venue & event - basis

## Schedule Details
* Frequency : Daily ; 10:30 A.M EST 
* Dependent Jobs (process_name ; process_id) : t_di_5a_le_udl_to_fds_mstr_load_dim_event_le_venue_event_info ; 9301,            t_di_5a_le_udl_to_fds_mstr_load_brdg_live_event_venue ; 9303

## Maintenance Log
* Date : 09/23/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5b Project.
{% enddocs %}

{% docs vw_aggr_cpg_daily_venue_sales %}
## Implementation Detail
*   Date        : 09/23/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_cpg_daily_venue_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Aggregated CPG Daily Venue Sales View consist of Sales details of WWE products on venue & event - basis

## Maintenance Log
* Date : 09/23/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5b Project.
{% enddocs %}

{% docs rpt_cp_daily_social_followership %}
## Implementation Detail
* Date        : 12/21/2020
* Version     : 1.0
* TableName    : rpt_cp_daily_social_followership
* Schema	  : fds_cp
* Contributor : Hima Dasan
* Description : rpt_cp_daily_social_followership provide Subscribers gains and followers of Facebook, YouTube, Instagram at country level 
## Maintenance Log
* Date : 12/21/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of network dashboards.
{% enddocs %}

{% docs vw_rpt_cp_daily_social_followership %}
## Implementation Detail
* Date        : 12/21/2020
* Version     : 1.0
* ViewName    : vw_rpt_cp_daily_social_followership
* Schema	  : fds_cp
* Contributor : Hima Dasan
* Description : rpt_cp_daily_social_followership provide Subscribers gains and followers of Facebook, YouTube, Instagram at country level 
## Maintenance Log
* Date : 12/21/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of network dashboards.
{% enddocs %}



{% docs rpt_mkt_weekly_owned_media_execution %}
## Implementation Detail
* Date        : 10/08/2020
* Version     : 1.0
* ViewName    : rpt_mkt_weekly_owned_media_execution
* Schema	  : fds_mkt
* Contributor : Lakshman Murugeshan
* Description : Reporting table for marketing team owned media execution. This is a weekly aggregate table containing the metrics Impressions, Viewership and Promos for the channels Owned YouTube, Owned Facebook, Owned Twitter, Owned Instagram, Owned TV US, Owned TV Promos, Owned TV Non US and Owned TV PPV.
## Maintenance Log
* Date : 09/23/2020 ; Developer: Lakshman Murugeshan ; Change: Initial Version 
## Maintenance Log
* Date : 11/04/2020 ; Developer: Hima Dasan ; Change: For Owned viewership data for USA , included smackdown program - 'WWE SMACKDOWN' to include all smackdown data.

{% enddocs %}


{% docs rpt_mkt_weekly_paid_media_execution %}
## Implementation Detail
* Date        : 10/08/2020
* Version     : 1.0
* ViewName    : rpt_mkt_weekly_paid_media_execution
* Schema	  : fds_mkt
* Contributor : Lakshman Murugeshan
* Description : Reporting table for marketing team paid media execution. This is a weekly aggregate table containing the metrics Impressions, Spend and Clicks for the channels Display, Search, YouTube, Twitter, Facebook and Snap chat.
## Maintenance Log
* Date : 09/23/2020 ; Developer: Lakshman Murugeshan ; Change: Initial Version 
{% enddocs %}


{% docs rpt_cp_weekly_consolidated_kpi %}
## Implementation Detail
* Date        : 10/13/2020
* Version     : 1.0
* ViewName    : rpt_cp_weekly_consolidated_kpi
* Schema	  : fds_cp
* Contributor : Lakshman Murugeshan
* Description : Reporting table is for consolidated KPI dashboard containing metrics related to Network subscriber and Digital platforms i.e. Youtube, Facebook, Twitter, Snapchat, Instagram and Doctom. This table is refreshed weekly as the metrics are aggregated weekly from Monday to Sunday.
## Maintenance Log
* Date : 10/13/2020 ; Developer: Lakshman Murugeshan ; Change: Initial Version 
{% enddocs %}


{% docs vw_rpt_cp_weekly_consolidated_kpi %}
## Implementation Detail
* Date        : 10/13/2020
* Version     : 1.0
* ViewName    : vw_rpt_cp_weekly_consolidated_kpi
* Schema	  : fds_cp
* Contributor : Lakshman Murugeshan
* Description : Reporting table is for consolidated KPI dashboard containing metrics related to Network subscriber and Digital platforms i.e. Youtube, Facebook, Twitter, Snapchat, Instagram and Doctom. This table is refreshed weekly as the metrics are aggregated weekly from Monday to Sunday.
## Maintenance Log
* Date : 10/13/2020 ; Developer: Lakshman Murugeshan ; Change: Initial Version 
{% enddocs %}


{% docs rpt_nplus_daily_ppv_streams %}

## Implementation Detail
* Date        : 10/27/2020
* Version     : 1.0
* TableName   : rpt_nplus_daily_ppv_streams
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : Report table consists of viewership by segment details for PPV Stream on daily-basis

## Schedule Details
* Frequency : Daily ; 
* Dependent Jobs (process_name ; process_id) :  

## Maintenance Log
* Date : 10/27/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.

{% enddocs %}


{% docs vw_rpt_nplus_daily_ppv_streams %}

## Implementation Detail
* Date        : 10/27/2020
* Version     : 1.0
* ViewName    : vw_rpt_nplus_daily_ppv_streams
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : View consists of viewership by segment details for PPV Stream  from aggregate table rpt_nplus_daily_ppv_streams on daily-basis

## Schedule Details
* Frequency : Daily ; 
* Dependent Jobs (process_name ; process_id) :  

## Maintenance Log
* Date : 10/27/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.

{% enddocs %}



{% docs rpt_nplus_daily_live_streams %}

## Implementation Detail
* Date        : 10/27/2020
* Version     : 1.0
* TableName   : rpt_nplus_daily_live_streams
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : Report table consists of viewership by segment details for Live Stream on daily-basis

## Schedule Details
* Frequency : Daily ; 
* Dependent Jobs (process_name ; process_id) :  

## Maintenance Log
* Date : 10/27/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.

{% enddocs %}



{% docs vw_rpt_nplus_daily_live_streams %}

## Implementation Detail
* Date        : 10/27/2020
* Version     : 1.0
* ViewName   : vw_rpt_nplus_daily_live_streams
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : View consists of viewership by segment details for Live Stream from aggregate table rpt_nplus_daily_live_streams on daily-basis

## Schedule Details
* Frequency : Daily ; 
* Dependent Jobs (process_name ; process_id) :  

## Maintenance Log
* Date : 10/27/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.

{% enddocs %}


{% docs rpt_nplus_daily_nxt_tko_streams %}

## Implementation Detail
* Date        : 10/27/2020
* Version     : 1.0
* TableName   : rpt_nplus_daily_nxt_tko_streams
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : Report table consists of viewership by segment details for NXT tko Stream on daily-basis

## Schedule Details
* Frequency : Daily ; 
* Dependent Jobs (process_name ; process_id) :  

## Maintenance Log
* Date : 10/27/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.

{% enddocs %}


{% docs vw_rpt_nplus_daily_nxt_tko_streams %}

## Implementation Detail
* Date        : 10/27/2020
* Version     : 1.0
* ViewName   : vw_rpt_nplus_daily_nxt_tko_streams
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : View consists of viewership by segment details for NXT tko Stream from aggregate table rpt_nplus_daily_nxt_tko_streams on daily-basis

## Schedule Details
* Frequency : Daily ; 
* Dependent Jobs (process_name ; process_id) :  

## Maintenance Log
* Date : 10/27/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.

{% enddocs %}


{% docs rpt_nplus_daily_milestone_complete_rates %}

## Implementation Detail
* Date        : 10/27/2020
* Version     : 1.0
* TableName   : rpt_nplus_daily_milestone_complete_rates
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : Report table consists of Complete rates and viewers for PPV,live and tko streams on daily-basis

## Schedule Details
* Frequency : Daily ; 
* Dependent Jobs (process_name ; process_id) :  

## Maintenance Log
* Date : 10/27/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.

{% enddocs %}


{% docs vw_rpt_nplus_daily_milestone_complete_rates %}

## Implementation Detail
* Date        : 10/27/2020
* Version     : 1.0
* ViewName   : vw_rpt_nplus_daily_milestone_complete_rates
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : View consists of Complete rates and viewers for PPV,live and tko streams from aggregate table rpt_nplus_daily_milestone_complete_rates on daily-basis

## Schedule Details
* Frequency : Daily ; 
* Dependent Jobs (process_name ; process_id) :  

## Maintenance Log
* Date : 10/27/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Network 2.0 Project.

{% enddocs %}


{% docs rpt_cpg_monthly_talent_overall_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_monthly_talent_overall_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Overall Shop Sales Report table consist of various sales details & metrics of talents on monthly-basis

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_monthly_talent_overall_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_monthly_talent_overall_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Overall Shop Sales Report view consist of various sales details & metrics of talents on monthly-basis referencing from Monthly Talent Overall Shop Sales Report table

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance Dashboard Views.
{% enddocs %}

{% docs rpt_cpg_weekly_talent_overall_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_weekly_talent_overall_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Weekly Talent Overall Shop Sales Report table consist of various sales details & metrics of talents on weekly-basis

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_weekly_talent_overall_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_weekly_talent_overall_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Weekly Talent Overall Shop Sales Report view consist of various sales details & metrics of talents on weekly-basis referencing from Weekly Talent Overall Shop Sales Report table

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance Dashboard Views.
{% enddocs %}

{% docs rpt_cpg_monthly_talent_top25_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_monthly_talent_top25_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Top 25 Shop Sales Report Table consist of various shop sales details & metrics of talents on each days of last month

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance & Top 25 Talent Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_monthly_talent_top25_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_monthly_talent_top25_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Top 25 Shop Sales Report view consist of various shop sales details & metrics of talents on each days of last month referencing from Monthly Talent Top 25 Shop Sales Report Table

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance & Top 25 Talent Dashboard Views.
{% enddocs %}

{% docs rpt_cpg_weekly_talent_top25_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_weekly_talent_top25_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Weekly Talent Top 25 Shop Sales Report Table consist of various shop sales details & metrics of talents on each days of last week

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance & Top 25 Talent Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_weekly_talent_top25_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_weekly_talent_top25_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Weekly Talent Top 25 Shop Sales Report view consist of various shop sales details & metrics of talents on each days of last week referencing from Weekly Talent Top 25 Shop Sales Report Table

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance & Top 25 Talent Dashboard Views.
{% enddocs %}

{% docs rpt_cpg_monthly_talent_top25_venue_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_monthly_talent_top25_venue_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Top 25 Venue Sales Report Table consist of various venue sales details & metrics of talents on each days of last month

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance & Top 25 Talent Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_monthly_talent_top25_venue_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_monthly_talent_top25_venue_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Top 25 Venue Sales Report view consist of various venue sales details & metrics of talents on each days of last month referencing from Monthly Talent Top 25 Venue Sales Report Table

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance & Top 25 Talent Dashboard Views.
{% enddocs %}

{% docs rpt_cpg_weekly_talent_top25_venue_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_weekly_talent_top25_venue_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Weekly Talent Top 25 Venue Sales Report Table consist of various venue sales details & metrics of talents on each days of last week

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance & Top 25 Talent Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_weekly_talent_top25_venue_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_weekly_talent_top25_venue_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Weekly Talent Top 25 Venue Sales Report view consist of various venue sales details & metrics of talents on each days of last week referencing from Weekly Talent Top 25 Venue Sales Report Table

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Weekly CPG Talent Performance & Top 25 Talent Dashboard Views.
{% enddocs %}


{% docs vw_rpt_daily_fb_published_post %}
## Implementation Detail
*   Date        : 10/29/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_daily_fb_published_post
*   Schema	    : fds_fbk
*   Contributor : Remya K Nair
*   Description : View contains only the most recent dim_video_id or dim_media_id or dim_story_id record from fds_fbk.fact_fb_published_post table   

## Maintenance Log
* Date : 10/29/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Cross Platform Project.
{% enddocs %}


{% docs vw_rpt_daily_fb_published_video %}
## Implementation Detail
*   Date        : 10/29/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_daily_fb_published_video
*   Schema	    : fds_fbk
*   Contributor : Remya K Nair
*   Description : View contains only the most recent dim_video_id or dim_media_id or dim_story_id record from fds_fbk.fact_fb_published_video table  

## Maintenance Log
* Date : 10/29/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Cross Platform Project.
{% enddocs %}


{% docs vw_rpt_daily_ig_published_frame %}
## Implementation Detail
*   Date        : 10/29/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_daily_ig_published_frame
*   Schema	    : fds_igm
*   Contributor : Remya K Nair
*   Description : View contains only the most recent dim_video_id or dim_media_id or dim_story_id record from fds_igm.fact_ig_published_frame table  

## Maintenance Log
* Date : 10/29/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Cross Platform Project.
{% enddocs %}


{% docs vw_rpt_daily_ig_published_post %}
## Implementation Detail
*   Date        : 10/29/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_daily_ig_published_post
*   Schema	    : fds_igm
*   Contributor : Remya K Nair
*   Description : View contains only the most recent dim_video_id or dim_media_id or dim_story_id record from fds_igm.fact_ig_published_post table  

## Maintenance Log
* Date : 10/29/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Cross Platform Project.
{% enddocs %}


{% docs vw_rpt_daily_ig_published_story %}
## Implementation Detail
*   Date        : 10/29/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_daily_ig_published_story
*   Schema	    : fds_igm
*   Contributor : Remya K Nair
*   Description : View contains only the most recent dim_video_id or dim_media_id or dim_story_id record from fds_igm.fact_ig_published_story table  

## Maintenance Log
* Date : 10/29/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Cross Platform Project.
{% enddocs %}


{% docs vw_rpt_daily_sc_published_story %}
## Implementation Detail
*   Date        : 10/30/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_daily_sc_published_story
*   Schema	    : fds_sc
*   Contributor : Remya K Nair
*   Description : View contains only the most recent dim_video_id or dim_media_id or dim_story_id record from fds_sc.fact_sc_published_story table  

## Maintenance Log
* Date : 10/30/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Cross Platform Project.
{% enddocs %}


{% docs vw_rpt_daily_sc_published_frame %}
## Implementation Detail
*   Date        : 10/30/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_daily_sc_published_frame
*   Schema	    : fds_sc
*   Contributor : Remya K Nair
*   Description : View contains only the most recent dim_video_id or dim_media_id or dim_story_id record from fds_sc.fact_sc_published_frame table  

## Maintenance Log
* Date : 10/30/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Cross Platform Project.
{% enddocs %}


{% docs vw_rpt_daily_tw_published_post %}
## Implementation Detail
*   Date        : 10/30/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_daily_tw_published_post
*   Schema	    : fds_tw
*   Contributor : Remya K Nair
*   Description : View contains only the most recent dim_video_id or dim_media_id or dim_story_id record from fds_tw.fact_tw_published_post table  

## Maintenance Log
* Date : 10/30/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of Cross Platform Project.
{% enddocs %}

{% docs rpt_cpg_daily_shop_kpi_orders %}
## Implementation Detail
*   Date        : 11/04/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_daily_shop_kpi_orders
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily CPG Shop KPI Orders Report table consist of order details of Daily CPG Shop Orders.

## Maintenance Log
* Date : 11/04/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily KPI Report Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_daily_shop_kpi_orders %}
## Implementation Detail
*   Date        : 11/04/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_daily_shop_kpi_orders
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily CPG Shop KPI Orders Report view consist of order details of Daily CPG Shop Orders referencing from Daily CPG Shop KPI Orders Report table

## Maintenance Log
* Date : 11/04/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily KPI Report Dashboard Views.
{% enddocs %}

{% docs rpt_cpg_daily_kpi_sale %}
## Implementation Detail
*   Date        : 11/04/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_daily_kpi_sale
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily CPG KPI Sale Report table consist of sale details of Daily CPG Shop & Venue Orders.

## Maintenance Log
* Date : 11/04/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily KPI Report Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_daily_kpi_sale %}
## Implementation Detail
*   Date        : 11/04/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_daily_kpi_sale
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily CPG KPI Sale Report view consist of sale details of Daily CPG Shop & Venue Orders referencing from Daily CPG KPI Sale Report table

## Maintenance Log
* Date : 11/04/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily KPI Report Dashboard Views.
{% enddocs %}


{% docs rpt_cp_monthly_global_followership_by_platform %}
## Implementation Detail
*   Date        : 11/05/2020
*   Version     : 1.0
*   ViewName    : rpt_cp_monthly_global_followership_by_platform
*   Schema	    : fds_cp
*   Contributor : Hima Dasan
*   Description : rpt_cp_monthly_global_followership_by_platform table consists of monthly followership details for platsforms - facebook,instagram,twitter,youtube and china social on country and account level.

## Maintenance Log
* Date : 11/05/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of monthly global market followership by platform.
{% enddocs %}


{% docs vw_rpt_cp_monthly_global_followership_by_platform %}
## Implementation Detail
*   Date        : 11/05/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cp_monthly_global_followership_by_platform
*   Schema	    : fds_cp
*   Contributor : Hima Dasan
*   Description : vw_rpt_cp_monthly_global_followership_by_platform view consists of monthly followership details for platsforms - facebook,instagram,twitter,youtube and china social on country and account level.

## Maintenance Log
* Date : 11/05/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of monthly global market followership by platform.
{% enddocs %}

{% docs rpt_cpg_daily_snapshot_order_sale %}
## Implementation Detail
*   Date        : 11/06/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_daily_snapshot_order_sale
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily CPG Snapshot Order Sale Report table consist of sale details of ordered units.

## Maintenance Log
* Date : 11/06/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily CPG Snapshot Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_daily_snapshot_order_sale %}
## Implementation Detail
*   Date        : 11/06/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_daily_snapshot_order_sale
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily CPG Snapshot Order Sale Report view consist of sale details of ordered units referencing from Daily CPG Snapshot Order Sale Report table

## Maintenance Log
* Date : 11/06/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily CPG Snapshot Dashboard Views.
{% enddocs %}

{% docs rpt_cpg_daily_snapshot_shipped_sale %}
## Implementation Detail
*   Date        : 11/06/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_daily_snapshot_shipped_sale
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily CPG Snapshot Shipped Sale Report table consist of sale details of shipped units.

## Maintenance Log
* Date : 11/06/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily CPG Snapshot Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_daily_snapshot_shipped_sale %}
## Implementation Detail
*   Date        : 11/06/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_daily_snapshot_shipped_sale
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily CPG Snapshot Shipped Sale Report view consist of sale details of shipped units referencing from Daily CPG Snapshot Shipped Sale Report table

## Maintenance Log
* Date : 11/06/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily CPG Snapshot Dashboard Views.
{% enddocs %}

{% docs rpt_cpg_daily_talent_brand_achievement %}
## Implementation Detail
*   Date        : 11/06/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_daily_talent_brand_achievement
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily Talent Brand Achievement Report table consist of achievement details of talent based on brands.

## Maintenance Log
* Date : 11/06/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily CPG Snapshot Dashboard Views.
{% enddocs %}

{% docs vw_rpt_cpg_daily_talent_brand_achievement %}
## Implementation Detail
*   Date        : 11/06/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_daily_talent_brand_achievement
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Daily Talent Brand Achievement Report view consist of achievement details of talent based on brands referencing from Daily Talent Brand Achievement Report table

## Maintenance Log
* Date : 11/06/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Daily CPG Snapshot Dashboard Views.
{% enddocs %}


{% docs rpt_cp_daily_int_kpi_ranking_dotcom %}
## Implementation Detail
*  Date : 11/13/2020
*  Version : 1.0
*  TableName : rpt_cp_daily_int_kpi_ranking_dotcom
*  Schema : fds_cp
*  Contributor : Hima Dasan
*  Description : Reporting table consists of  daily international KPI ranking data for Dotcom .Has metrics like unique_visitors,visits,page_views,video_views,hours_watched,population.
*  JIRA : PSTA-1538
## Maintenance Log
* Date : 11/13/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of international KPI ranking Dotcom.
{% enddocs %}

{% docs vw_rpt_cp_daily_int_kpi_ranking_dotcom %}
## Implementation Detail
*  Date : 11/13/2020
*  Version : 1.0
*  ViewName : vw_rpt_cp_daily_int_kpi_ranking_dotcom
*  Schema : fds_cp
*  Contributor : Hima Dasan
*  Description : Reporting view consists of  daily international KPI ranking data for Dotcom .Has metrics like unique_visitors,visits,page_views,video_views,hours_watched,population.
*  JIRA : PSTA-1538
## Maintenance Log
* Date : 11/13/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of international KPI ranking Dotcom.
{% enddocs %}

{% docs rpt_cp_daily_int_kpi_ranking_network%}
## Implementation Detail
*  Date : 11/13/2020
*  Version : 1.0
*  TableName : rpt_cp_daily_int_kpi_ranking_network
*  Schema : fds_cp
*  Contributor : Hima Dasan
*  Description : Reporting table consists of  daily international KPI ranking data for network .Has metrics like active_subs,hours_watched,population.
*  JIRA : PSTA-1538
## Maintenance Log
* Date : 11/13/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of international KPI ranking Network.
* Date : 12/09/2020 ; Developer: Hima Dasan ; Change: resolving nulls values for hours_Watched metric in network view - jira -PSTA-1905 .

{% enddocs %}

{% docs vw_rpt_cp_daily_int_kpi_ranking_network%}
## Implementation Detail
*  Date : 11/13/2020
*  Version : 1.0
*  ViewName : vw_rpt_cp_daily_int_kpi_ranking_network
*  Schema : fds_cp
*  Contributor : Hima Dasan
*  Description : Reporting view consists of  daily international KPI ranking data for network .Has metrics like active_subs,hours_watched,population.
*  JIRA : PSTA-1538
## Maintenance Log
* Date : 11/13/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of international KPI ranking Network.
*Date : 1/6/2021 ; Developer: Hima Dasan ; Change: Added views as part of enhancement on network KPI ranking . Jira - PSTA-1897
{% enddocs %}


{% docs rpt_cp_daily_int_kpi_ranking_tv%}
## Implementation Detail
*  Date : 11/13/2020
*  Version : 1.0
*  TableName : rpt_cp_daily_int_kpi_ranking_tv
*  Schema : fds_cp
*  Contributor : Hima Dasan
*  Description : Reporting table consists of  daily international KPI ranking data for TV .Has metrics like telecasts,weekly_aud,duration_mins,population.
*  JIRA : PSTA-1538
## Maintenance Log
* Date : 11/13/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of international KPI ranking TV.
*Date : 1/6/2021 ; Developer: Hima Dasan ; Change: Added column series_type and telecast_hours as part of KPI ranking tv enhancement . Jira - PSTA-1897 
{% enddocs %}

{% docs vw_rpt_cp_daily_int_kpi_ranking_tv%}
## Implementation Detail
*  Date : 11/13/2020
*  Version : 1.0
*  ViewName : vw_rpt_cp_daily_int_kpi_ranking_tv
*  Schema : fds_cp
*  Contributor : Hima Dasan
*  Description : Reporting view consists of  daily international KPI ranking data for TV .Has metrics like telecasts,weekly_aud,duration_mins,population.
*  JIRA : PSTA-1538
## Maintenance Log
* Date : 11/13/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of international KPI ranking TV.
{% enddocs %}

{% docs rpt_cp_daily_int_kpi_ranking_youtube%}
## Implementation Detail
*  Date : 11/13/2020
*  Version : 1.0
*  TableName : rpt_cp_daily_int_kpi_ranking_youtube
*  Schema : fds_cp
*  Contributor : Hima Dasan
*  Description : Reporting table consists of  daily international KPI ranking data for Youtube .Has metrics like views,netsubs,hours_watched,revenue,gross_revenue,population.
*  JIRA : PSTA-1538
## Maintenance Log
* Date : 11/13/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of international KPI ranking TV.
{% enddocs %}

{% docs vw_rpt_cp_daily_int_kpi_ranking_youtube%}
## Implementation Detail
*  Date : 11/13/2020
*  Version : 1.0
*  ViewName : vw_rpt_cp_daily_int_kpi_ranking_youtube
*  Schema : fds_cp
*  Contributor : Hima Dasan
*  Description : Reporting view consists of  daily international KPI ranking data for Youtube .Has metrics like views,netsubs,hours_watched,revenue,gross_revenue,population.
*  JIRA : PSTA-1538
## Maintenance Log
* Date : 11/13/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of international KPI ranking TV.
{% enddocs %}

{% docs aggr_voc_daily_mentions_count %}
## Implementation Detail
*   Date        : 11/23/2020
*   Version     : 1.0
*   TableName   : aggr_voc_daily_mentions_count
*   Schema	    : fds_voc
*   Contributor : Rahul Chandran
*   Description : Daily Aggregate VOC Mentions Count Table provides count of tweets about mentions on daily-basis

## Maintenance Log
* Date : 11/23/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 8 Project.
{% enddocs %}

{% docs vw_aggr_voc_daily_mentions_count %}
## Implementation Detail
*   Date        : 11/23/2020
*   Version     : 1.0
*   ViewName    : vw_aggr_voc_daily_mentions_count
*   Schema	    : fds_voc
*   Contributor : Rahul Chandran
*   Description : Daily Aggregate VOC Mentions Count view provides count of tweets about mentions referencing from Daily Aggregate VOC Mentions Count Table

## Maintenance Log
* Date : 11/23/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 8 Project.
{% enddocs %}

{% docs rpt_cp_daily_followership_by_platform_digitalsnapshot %}
## Implementation Detail
*   Date        : 11/24/2020
*   Version     : 1.0
*   ViewName    : rpt_cp_daily_followership_by_platform_digitalsnapshot
*   Schema	    : fds_cp
*   Contributor : Remya K Nair
*   Description : rpt_cp_daily_followership_by_platform_digitalsnapshot table consists of followership details for platforms - social media,youtube and dotcom on country level.

## Maintenance Log
* Date : 11/24/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of cross platform Monthly International Market Viewership.
{% enddocs %}

{% docs vw_rpt_cp_daily_followership_by_platform_digitalsnapshot %}
## Implementation Detail
*   Date        : 11/24/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cp_daily_followership_by_platform_digitalsnapshot
*   Schema	    : fds_cp
*   Contributor : Remya K Nair
*   Description : vw_rpt_cp_daily_followership_by_platform_digitalsnapshot consists of followership details for platforms - social media,youtube and dotcom on country level.

## Maintenance Log
* Date : 11/24/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of cross platform Monthly International Market Viewership.
{% enddocs %}

{% docs drvd_intra_hour_quarter_hour_adds %}
## Implementation Detail
*   Date        : 11/20/2020
*   Version     : 1.0
*   ViewName    : drvd_intra_hour_quarter_hour_adds
*   Schema	    : fds_nplus
*   Contributor : Sudhakar Andugula
*   Description : Intra hour table and Table workbook refresh tto capture every hour , each quarter paid and trial adds info
*   Frequency   : Every 15 mins

## Maintenance Log
* Date : 11/20/2020 ; Developer: Sudhakar Andugula ; Change: Initial Version as a part Intra hour dashboard
{% enddocs %}

{% docs vw_rpt_cp_weekly_consolidated_kpi_visuals %}
## Implementation Detail
*   Date        : 11/30/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cp_weekly_consolidated_kpi_visuals
*   Schema	    : fds_cp
*   Contributor : Hima Dasan
*   Description : Consolidated KPI weekly reporting view for graphs contains metrics from network ,live events and tv


## Maintenance Log
* Date : 11/30/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part Weekly consolidated kpi view for graphs
{% enddocs %}


{% docs vw_aggr_nplus_ppv_week_adds_tracking_hist %}
## Implementation Detail
* Date        : 12/04/2020
* Version     : 1.0
* ViewName    : vw_aggr_nplus_ppv_week_adds_tracking_hist
* Schema	  : fds_nplus
* Contributor : Hima Dasan
* Description : vw_aggr_nplus_ppv_week_adds_tracking_hist view consists of historical PPV event details with count of paid/trail/promo subscription add and forecast suscription count for PPV summary historical tab of GHW report.
## Maintenance Log
* Date : 12/04/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Network 2.0 Project.
{% enddocs %}


{% docs rpt_cpg_monthly_royalty %}
## Implementation Detail
* Date        : 12/07/2020
* Version     : 1.0
* TableName    : rpt_cpg_monthly_royalty
* Schema	  : fds_cpg
* Contributor : Hima Dasan
* Description : Reporting table consists of monthly royalty finance details . Has item shipped,returned and refunded details of items in monthly basis.

## Maintenance Log
* Date : 12/07/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of cpg royalty report finance.
{% enddocs %}

{% docs vw_rpt_cpg_monthly_royalty %}
## Implementation Detail
* Date        : 12/07/2020
* Version     : 1.0
* ViewName    : vw_rpt_cpg_monthly_royalty
* Schema	  : fds_cpg
* Contributor : Hima Dasan
* Description : Reporting view consists of monthly royalty finance details . Has item shipped,returned and refunded details of items in monthly basis.

## Maintenance Log
* Date : 12/07/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of cpg royalty report finance.
{% enddocs %}


{% docs rpt_cp_weekly_social_followership %}
## Implementation Detail
* Date        : 11/04/2020
* Version     : 1.0
* ViewName    : rpt_cp_weekly_social_followership
* Schema	  : fds_cpg
* Contributor : Sandeep Battula
* Description : Weekly Social Followership reporting table consists of social followership data for conviva platforms and China social platforms. It also stores account_name, designation, brand, gender for the talent.

## Maintenance Log
* Date : 11/04/2020 ; PSTA-1283 Developer: Sandeep Battula ; Change: Initial Version as a part of Weekly Social Followership reporting.
{% enddocs %}

{% docs rpt_cp_monthly_social_overview %}
## Implementation Detail
* Date        : 11/13/2020
* Version     : 1.0
* ViewName    : rpt_cp_monthly_social_overview
* Schema	  : fds_cpg
* Contributor : Sandeep Battula
* Description : Monthly Social reporting table consists of social consumption, engagemenet and followership data for social platforms. It also has corresponding account name, country and region details.

## Maintenance Log
* Date : 11/13/2020 ; PSTA-1812 Developer: Sandeep Battula ; Change: Initial Version as a part of Monthly Social reporting.
{% enddocs %}

{% docs vw_rpt_cp_weekly_social_followers_by_account %}
## Implementation Detail
* Date        : 11/13/2020
* Version     : 1.0
* ViewName    : vw_rpt_cp_weekly_social_followers_by_account
* Schema	  : fds_cpg
* Contributor : Sandeep Battula
* Description : Weekly Social Followers by account view is built on top of reporting table-rpt_cp_weekly_social_followership to fetch the followers at the granularity of account. The followers for previous week, previous month and previous year are also calculated.

## Maintenance Log
* Date : 11/04/2020 ; Developer: Sandeep Battula ; Change: Initial Version as a part of Weekly Social Followers by account JIRA-PSTA-1047.
{% enddocs %}

{% docs vw_rpt_cp_weekly_social_followers_by_platform %}
## Implementation Detail
* Date        : 11/13/2020
* Version     : 1.0
* ViewName    : vw_rpt_cp_weekly_social_followers_by_platform
* Schema	  : fds_cpg
* Contributor : Sandeep Battula
* Description : Weekly Social Followers by platform view is built on top of reporting table-rpt_cp_weekly_social_followership to fetch the  followers by platform. The followers for previous week, previous year and previous month are also calculated.

## Maintenance Log
* Date : 11/04/2020 ; Developer: Sandeep Battula ; Change: Initial Version as a part of Weekly Social Followers by platform JIRA-PSTA-1047.
{% enddocs %}

{% docs vw_rpt_cp_weekly_social_followers_summary %}
## Implementation Detail
* Date        : 11/13/2020
* Version     : 1.0
* ViewName    : vw_rpt_cp_weekly_social_followers_summary
* Schema	  : fds_cpg
* Contributor : Sandeep Battula
* Description : Weekly Social Followers summary view is built on top of reporting table-rpt_cp_weekly_social_followership to fetch the summary of followers. The followers for previous week and previous month are also calculated.

## Maintenance Log
* Date : 11/04/2020 ; Developer: Sandeep Battula ; Change: Initial Version as a part of Weekly Social Followers summary JIRA-PSTA-1047
{% enddocs %}


{% docs rpt_yt_daily_consumption_bychannel %}
## Implementation Detail
* Date        : 12/21/2020
* Version     : 1.0
* TableName    : rpt_yt_daily_consumption_bychannel
* Schema	  : fds_yt
* Contributor : Hima Dasan
* Description : Reporting table consists of daily youtube consumption by channel details . Has metrics like views,likes,dislikes,watched minutes,subscribers gained and lost,revenue views etc for respective country ,channel,content type on daily basis.

## Maintenance Log
* Date : 12/21/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of youtube consumption by channel.
{% enddocs %}

{% enddocs %}

{% docs vw_rpt_yt_daily_consumption_bychannel %}
## Implementation Detail
* Date        : 12/21/2020
* Version     : 1.0
* ViewName    : vw_rpt_yt_daily_consumption_bychannel
* Schema	  : fds_yt
* Contributor : Hima Dasan
* Description : Reporting view consists of daily youtube consumption by channel details . Has metrics like views,likes,dislikes,watched minutes,subscribers gained and lost,revenue views etc for respective country ,channel,content type on daily basis.

## Maintenance Log
* Date : 12/21/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of youtube consumption by channel.
{% enddocs %}

{% docs rpt_yt_daily_wwe_video %}
## Implementation Detail
* Date        : 12/24/2020
* Version     : 1.0
* ViewName    : rpt_yt_daily_wwe_video
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : Report table consists of total views  at video_id,region and talent  level on daily basis

## Maintenance Log
* Date : 12/24/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}

{% docs rpt_yt_daily_wwe_talent %}
## Implementation Detail
* Date        : 12/24/2020
* Version     : 1.0
* ViewName    : rpt_yt_daily_wwe_talent
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : Report table consists of splitting talents present in talent pool on daily basis

## Maintenance Log
* Date : 12/24/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}

{% docs rpt_yt_daily_wwe_talent_views %}
## Implementation Detail
* Date        : 12/24/2020
* Version     : 1.0
* ViewName    : rpt_yt_daily_wwe_talent_views
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : Report table consists of total views for individual talends at video_id level

## Maintenance Log
* Date : 12/24/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}

{% docs vw_rpt_yt_daily_wwe_talent %}
## Implementation Detail
* Date        : 12/24/2020
* Version     : 1.0
* ViewName    : vw_rpt_yt_daily_wwe_talent
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : View consists of total views ,total videos,popularity index,unique index for individual talends at region  level and granularity base

## Maintenance Log
* Date : 12/24/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}

{% docs vw_rpt_nplus_weekly_network_kpi %}
## Implementation Detail
* Date        : 01/12/2021
* Version     : 1.0
* ViewName    : vw_rpt_nplus_weekly_network_kpi
* Schema	  : fds_nplus
* Contributor : Remya K Nair
* Description : View consists of subscriber adds and loss based on payment methods/order type ,registered users,subscriptions based on payment method etc.
## Maintenance Log
* Date : 01/12/2021 ; Developer: Remya K Nair ; Change: Initial Version as a part of  N/W 2.0 project.
{% enddocs %}


{% docs aggr_cpg_daily_shops_sessions %}

## Implementation Detail
* Date        : 1/12/2021
* Version     : 1.0
* TableName   : aggr_cpg_daily_shops_sessions
* Schema	  : fds_cpg
* Contributor : Shihab, Dhanesh
* Description : WWE Shops Daily Sessions Transaction and Revenue Summary

## Schedule Details
* Frequency : Daily ; 10:00 A.M EST
* Dependent Jobs (process_name ; process_id) : dbt_aggr_cpg_daily_shops_sessions ; 30601 

## Maintenance Log
* Date : 1/12/2021 ; Developer: Shihab, Dhanesh ; Change: Initial Version as a part of Phase 5b Project. 
{% enddocs %}


{% docs vw_rpt_cpg_monthly_shops_sessions %}

## Implementation Detail
* Date        : 1/12/2021
* Version     : 1.0
* TableName   : vw_rpt_cpg_monthly_shops_sessions
* Schema	  : fds_cpg
* Contributor : Shihab, Dhanesh
* Description : WWE Shops Sessions - Monthly Conversions and AOV Summary 

## Maintenance Log
* Date : 1/12/2021 ; Developer: Shihab, Dhanesh ; Change: Initial Version as a part of Phase 5b Project. 
{% enddocs %}


{% docs vw_rpt_cpg_weekly_shops_sessions %}

### Implementation Detail
* Date        : 1/12/2021
* Version     : 1.0
* TableName   : vw_rpt_cpg_weekly_shops_sessions
* Schema	  : fds_cpg
* Contributor : Shihab, Dhanesh
* Description : WWE Shops Sessions - Weekly Conversions and AOV Summary 

## Maintenance Log
* Date : 1/12/2021 ; Developer: Shihab, Dhanesh ; Change: Initial Version as a part of Phase 5b Project. 
{% enddocs %}

{% docs rpt_tv_weekly_consolidated_kpi %}
## Implementation Detail
* Date        : 1/12/2022
* Version     : 1.0
* ViewName    : rpt_tv_weekly_consolidated_kpi
* Schema	  : fds_nl
* Contributor : Lakshman Murugeshan
* Description : Reporting table is for consolidated KPI dashboard containing metrics related to Network subscriber and Digital platforms i.e. Youtube, Facebook, Twitter, Snapchat, Instagram and Doctom. This table is refreshed weekly as the metrics are aggregated weekly from Monday to Sunday.
## Maintenance Log
* Date : 1/12/2021 ; Developer: Lakshman Murugeshan ; Change: Initial Version 
{% enddocs %}

{% docs rpt_yt_daily_wwe_talent_views_pastyear %}
## Implementation Detail
* Date        : 01/15/2021
* Version     : 1.0
* TableName    : rpt_yt_daily_wwe_talent_views_pastyear
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : Report table consists of total views,views for 30 days from uploaded date,video count for individual talents against country for previous year
## Maintenance Log
* Date : 01/15/2021 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}

{% docs rpt_yt_daily_wwe_talent_views_pastquarter %}
## Implementation Detail
* Date        : 01/15/2021
* Version     : 1.0
* TableName    : rpt_yt_daily_wwe_talent_views_pastquarter
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : Report table consists of total views,views for 30 days from uploaded date,video count for individual talents against country for previous quarter
## Maintenance Log
* Date : 01/15/2021 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}

{% docs rpt_yt_daily_wwe_talent_views_halfyear %}
## Implementation Detail
* Date        : 01/15/2021
* Version     : 1.0
* TableName    : rpt_yt_daily_wwe_talent_views_halfyear
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : Report table consists of total views,views for 30 days from uploaded date,video count for individual talents against country for past six months
## Maintenance Log
* Date : 01/15/2021 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}

{% docs rpt_yt_daily_wwe_talent_views_since_upload %}
## Implementation Detail
* Date        : 01/15/2021
* Version     : 1.0
* TableName    : rpt_yt_daily_wwe_talent_views_since_upload
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : Report table consists of total views,views for 30 days from uploaded date,video count for individual talents against country since upload
## Maintenance Log
* Date : 01/15/2021 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}

{% docs vw_rpt_yt_daily_wwe_talent_views_indices %}
## Implementation Detail
* Date        : 01/15/2021
* Version     : 1.0
* ViewName    : vw_rpt_yt_daily_wwe_talent_views_indices
* Schema	  : fds_yt
* Contributor : Remya K Nair
* Description : View consists of total views,views for 30 days from uploaded date,video count based on granularity,unique index and popularity index based on total_views and views_30days for individual talents against country on granularity basis                        
## Maintenance Log
* Date : 01/15/2021 ; Developer: Remya K Nair ; Change: Initial Version as a part of  YT viewership by Talent by Country.
{% enddocs %}
