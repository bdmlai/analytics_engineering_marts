{% docs rpt_nl_daily_wwe_live_commercial_ratings %}

## Implemenation Detail
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

## Implemenation Detail
*   Date        : 06/12/2020
*   Version     : 1.0
*   TableName   : rpt_nl_daily_wwe_live_quarterhour_ratings
*   Schema	    : fds_nl
*   Contributor : Sudhakar Andugula
*   Description : WWE Live Quarter Hour Ratings Daily Report table consist of rating details of all WWE Live - RAW, SD, NXT Programs referencing from Quarter Hour Viewership Ratings Table on daily-basis 

## Schedule Details
* Frequency : Daily ; 02:00 A.M EST (Wed-Mon) & 04:00 A.M EST (Tue)
* Dependent Jobs (process_name ; process_id) : : t_di_nielsen_fact_nl_quarterhour_viewership_ratings_daily_slot2_abac ; 12145 (Wed-Mon) & t_di_nielsen_fact_nl_quarterhour_viewership_ratings_abac ; 12122 (Tue)

## Maintenance Log
* Date : 06/12/2020 ; Developer: Sudhakar Andugula ; Change: Initial Version as a part of Phase 4b Project.

{% enddocs %}



{% docs rpt_nl_daily_wwe_program_ratings %}
## Implemenation Detail
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

## Implemenation Detail
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


{% enddocs %}



{% docs vw_rpt_nl_weekly_channel_switch %}

## Implemenation Detail
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

## Implemenation Detail
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
* Contributor : Sudhakar Andugula/Remya K Nair
* Description :vw_rpt_nl_weekly_overlap_program_4_way_oob view consists of  Schedule (Both Staright Neilsen Run and Derived) details  and calculations for Overlap data for  WWE, AEW and other wrestling programs 

## Maintenance Log
* Date : 06/19/2020 ; Developer: Sudhakar Andugula/Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.


{% enddocs %}



{% docs vw_rpt_nl_weekly_overlap_program_4_way_oob %}

## Implementation Detail
* Date        : 06/19/2020
* Version     : 1.0
* ViewName    : vw_rpt_nl_weekly_overlap_program_4_way_oob
* Schema	  : fds_nl
* Contributor : Sudhakar Andugula/Remya K Nair
* Description : vw_rpt_nl_weekly_overlap_program_4_way_oob view consists of  Schedule (Both Staright Neilsen Run and Derived) details  and calculations for Overlap data for  WWE, AEW and other wrestling programs
 
## Maintenance Log
* Date : 06/19/2020 ; Developer: Sudhakar Andugula/Remya K Nair ; Change: Initial Version as a part of Phase 4b Project.


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
* Version     : 1.0
* ViewName    : aggr_cp_weekly_consumption_by_platform
* Schema	  : fds_cp
* Contributor : Sandeep Battula
* Description : aggr_cp_weekly_consumption_by_platform This aggregate table stores the crossplatform consumption metrics - total views and total minutes watched aggregated for each week for platforms- Youtube, facebook, Twitter, Instagram, Snapchat and dotcom/App.
## Maintenance Log
* Date : 06/21/2020 ; Developer: Code: Sandeep Battula, DBT: Sudhakar ; Change: Initial Version as a part of network dashboards.
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
