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
*   Schema	: fds_kntr
*   Contributor : Hima Dasan
*   Description : View calculates actual viewing hour on monthly basis and calculates estimate value for last month for all WWE programs

## Maintenance Log
* Date : 07/24/2020 ; Developer: Hima Dasan ; Change: Initial Version as a part of Phase 4b Project.
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