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
* Date : 08/06/2020 ; Developer: Remya K Nair ; Change:TAB-2105,TAB-2106 :- Added src_series_name and excluded all the repeats while calculating the live viewership by including src_program_attributes filter..

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

{% docs vw_aggr_cp_monthly_social_followership %}
## Implementation Detail
* Date        : 09/23/2020
* Version     : 1.0
* ViewName    : vw_aggr_cp_monthly_social_followership
* Schema	  : fds_cp
* Contributor : Remya K Nair
* Description : vw_aggr_cp_monthly_social_followership provide Subscribers gains and followers of Facebook, YouTube, Instagram at country level 
## Maintenance Log
* Date : 09/23/2020 ; Developer: Remya K Nair ; Change: Initial Version as a part of network dashboards.
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

{% docs rpt_cpg_monthly_talent_overall_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   TableName   : rpt_cpg_monthly_talent_overall_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Overall Shop Sales Report table consist of various sales details & metrics of talents on monthly-basis

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5B Project.
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
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5B Project.
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
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5B Project.
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
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5B Project.
{% enddocs %}

{% docs vw_rpt_cpg_monthly_talent_top25_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_monthly_talent_top25_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Top 25 Shop Sales Report view consist of various shop sales details & metrics of talents on each days of last month

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5B Project.
{% enddocs %}

{% docs vw_rpt_cpg_weekly_talent_top25_shop_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_weekly_talent_top25_shop_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Weekly Talent Top 25 Shop Sales Report view consist of various shop sales details & metrics of talents on each days of last week

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5B Project.
{% enddocs %}

{% docs vw_rpt_cpg_monthly_talent_top25_venue_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_monthly_talent_top25_venue_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Monthly Talent Top 25 Venue Sales Report view consist of various venue sales details & metrics of talents on each days of last month

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5B Project.
{% enddocs %}

{% docs vw_rpt_cpg_weekly_talent_top25_venue_sales %}
## Implementation Detail
*   Date        : 10/16/2020
*   Version     : 1.0
*   ViewName    : vw_rpt_cpg_weekly_talent_top25_venue_sales
*   Schema	    : fds_cpg
*   Contributor : Rahul Chandran
*   Description : Weekly Talent Top 25 Venue Sales Report view consist of various venue sales details & metrics of talents on each days of last week

## Maintenance Log
* Date : 10/16/2020 ; Developer: Rahul Chandran ; Change: Initial Version as a part of Phase 5B Project.
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






