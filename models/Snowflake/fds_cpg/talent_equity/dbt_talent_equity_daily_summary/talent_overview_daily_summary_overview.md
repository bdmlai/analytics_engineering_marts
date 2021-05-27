{% docs rpt_daily_talent_equity_centralized_table_documentation %}

Model Overview :
    Objective -  
        Understand the return on creative investments made at a talent level. Benchmark/compare Talent KPIs across Talents. Use the variables/benchmarks to forecast Talent KPIs (future)  

        Return defined as – Social engagement (FB/Twitter engagement, consumption), YouTube viewership, CPG Sales at a daily level  
        Creative investment defined as – Appearance across shows, Designation, Achievement, Absence, heel/face at a daily level  

    Description-
        Using DBT  Models + Analysis to optimize and contextulize talent report for Talent Mangement Team. This means looking daily changes of talent kpi cross EMM, content, Social media, TV and CPG to undertand the performance at talent, daily level.  
        * Create dbt models to organize data from all 5 sources and merge into on centralized table by talent and day  
        * benchmarking and comparison of talent performance across time   
        * forecast talent performance across KPIs and understand driving factor  
    
    For detailed metrics, please go through Pipeline Overview below. For column description, please go through  Columns section  
     
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             DBT Cloud
    - Base Models : 
                            - base_daily_talent_absence_table 
                            - base_daily_talent_achievement_table 
                            - base_daily_talent_appearance_table
                            - base_daily_talent_brand_table
                            - base_daily_talent_cpg_shop_table
                            - base_daily_talent_cpg_venue_table
                            - base_daily_talent_fb_post_table
                            - base_daily_talent_fb_vids_table
                            - base_daily_talent_heelface_table
                            - base_daily_talent_ig_post_table
                            - base_daily_talent_ig_vids_table
                            - base_daily_talent_mentions_table
                            - base_daily_talent_sm_followership_table
                            - base_daily_talent_table
                            - base_daily_talent_tv_rating_post_table
                            - base_daily_talent_tw_post_table
                            - base_daily_talent_tw_vids_table
                            - base_daily_talent_yt_viewership_table

    - Intermediate Models : 
                            - intm_daily_talent_attribute_table 
                            - intm_daily_talent_achievement_table 
                            - intm_daily_talent_appearance_table
                            - intm_daily_talent_cpg_table
                            - intm_daily_talent_fb_post_table
                            - intm_daily_talent_fb_vids_table
                            - intm_daily_talent_ig_post_table
                            - intm_daily_talent_ig_vids_table
                            - intm_daily_talent_mentions_table
                            - intm_daily_talent_sm_followership_table
                            - intm_daily_talent_tv_rating_post_table
                            - intm_daily_talent_tw_post_table
                            - intm_daily_talent_tw_vids_table
                            - intm_daily_talent_yt_split_table

    - Summary Models : 
                            - summ_daily_talent_yt_split_table 
                            - summ_daily_talent_yt_viewership_table 
    - Final Models : 
                            - rpt_daily_talent_equity_centralized_table                       

    - Deployment Environment: COE Analytics Engineering Deployment
    - Schedule Name:          talent_equity_daily_summary_job
    - Schedule Frequency :    Daily

{% enddocs rpt_daily_talent_equity_centralized_table_documentation %}

{% docs base_daily_talent_absence_table_documentation %}

Model Overview :
    -   This table contains absence at daily talent level 

Instruction on How to Run the Model
    -   dbt run --models tag:'daily'    

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :           base_daily_talent_absence_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_absence_table_documentation %}

{% docs base_daily_talent_brand_table_documentation %}

Model Overview :
    -   This table contains brand at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :           base_daily_talent_brand_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_brand_table_documentation %}

{% docs base_daily_talent_heelface_table_documentation %}

Model Overview :
    -   This table contains heelface at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :           base_daily_talent_heelface_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_heelface_table_documentation %}

{% docs base_daily_talent_achievement_table_documentation %}

Model Overview :
    -   This table contains achievement at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :           base_daily_talent_achievement_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_achievement_table_documentation %}

{% docs base_daily_talent_table_documentation %}

Model Overview :
    -   This table contains cross join for all talents and date

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :           base_daily_talent_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_table_documentation %}

{% docs base_daily_talent_appearance_table_documentation %}

Model Overview :
    -   This table contains segment wsie talent screen time across episodes

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :           base_daily_talent_appearance_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_appearance_table_documentation %}

{% docs intm_daily_talent_appearance_table_documentation %}

Model Overview :
    -   This table contains segment wsie talent screen time across episodes

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_appearance_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_appearance_table_documentation %}

{% docs intm_daily_talent_achievement_table_documentation %}

Model Overview :
    -   This table contains achievement at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_achievement_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_achievement_table_documentation %}

{% docs intm_daily_talent_brand_designation_table_documentation %}

Model Overview :
    -   This table contains brand, heelface and absence at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_brand_heelface_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_brand_designation_table_documentation %}

{% docs base_daily_talent_cpg_shop_table_documentation %}

Model Overview :
    -   This table contains cpg shop sales at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_cpg_shop_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_cpg_shop_table_documentation %}

{% docs base_daily_talent_cpg_venue_table_documentation %}

Model Overview :
    -   This table contains cpg venue sales at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_cpg_venue_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_cpg_venue_table_documentation %}

{% docs intm_daily_talent_cpg_table_documentation %}

Model Overview :
    -   This table contains cpg sales at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_cpg_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_cpg_table_documentation %}

{% docs base_daily_talent_fb_post_table_documentation %}

Model Overview :
    -   This table contains fb post engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_fb_post_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_fb_post_table_documentation %}

{% docs base_daily_talent_ig_post_table_documentation %}

Model Overview :
    -   This table contains ig post engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_ig_post_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_ig_post_table_documentation %}

{% docs base_daily_talent_tw_post_table_documentation %}

Model Overview :
    -   This table contains tw post engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_tw_post_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_tw_post_table_documentation %}

{% docs base_daily_talent_tw_vids_table_documentation %}

Model Overview :
    -   This table contains tw video engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_tw_vids_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_tw_vids_table_documentation %}

{% docs base_daily_talent_ig_vids_table_documentation %}

Model Overview :
    -   This table contains ig video engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_ig_vids_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_ig_vids_table_documentation %}

{% docs base_daily_talent_fb_vids_table_documentation %}

Model Overview :
    -   This table contains fb video engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_fb_vids_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_fb_vids_table_documentation %}

{% docs intm_daily_talent_fb_post_table_documentation %}

Model Overview :
    -   This table contains fb post engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_fb_post_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_fb_post_table_documentation %}

{% docs intm_daily_talent_ig_post_table_documentation %}

Model Overview :
    -   This table contains ig post engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_ig_post_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_ig_post_table_documentation %}

{% docs intm_daily_talent_tw_post_table_documentation %}

Model Overview :
    -   This table contains tw post engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_tw_post_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_tw_post_table_documentation %}

{% docs intm_daily_talent_tw_vids_table_documentation %}

Model Overview :
    -   This table contains tw video engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_tw_vids_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_tw_vids_table_documentation %}

{% docs intm_daily_talent_ig_vids_table_documentation %}

Model Overview :
    -   This table contains ig video engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_ig_vids_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_ig_vids_table_documentation %}

{% docs intm_daily_talent_fb_vids_table_documentation %}

Model Overview :
    -   This table contains fb video engagements at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_fb_vids_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_fb_vids_table_documentation %}

{% docs base_daily_talent_sm_followership_table_documentation %}

Model Overview :
    -   This table contains social media followership at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_sm_followership_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_sm_followership_table_documentation %}

{% docs intm_daily_talent_sm_followership_table_documentation %}

Model Overview :
    -   This table contains social media followership at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_sm_followership_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_sm_followership_table_documentation %}

{% docs base_daily_talent_tv_rating_table_documentation %}

Model Overview :
    -   This table contains TV rating data at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           base_daily_talent_tv_rating_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_tv_rating_table_documentation %}

{% docs intm_daily_talent_tv_rating_table_documentation %}

Model Overview :
    -   This table contains TV rating data at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_tv_rating_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_tv_rating_table_documentation %}


{% docs intm_daily_talent_twitter_mentions_table_documentation %}

Model Overview :
    -   This table contains Twitter mentions data at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Dataintm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - intm Models :           NA
    - Final Model :           intm_daily_talent_twitter_mentions_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_twitter_mentions_table_documentation %}


{% docs base_daily_talent_twitter_mentions_table_documentation %}

Model Overview :
    -   This table contains Twitter mentions data at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - base Models :           NA
    - Final Model :           base_daily_talent_twitter_mentions_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_twitter_mentions_table_documentation %}

{% docs base_daily_talent_yt_viewership_table_documentation %}

Model Overview :
    -   Base table containing YT viewership data at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - base Models :           NA
    - Final Model :           base_daily_talent_yt_viewership_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs base_daily_talent_yt_viewership_table_documentation %}

{% docs summ_daily_talent_yt_viewership_table_documentation %}

Model Overview :
    -   Summary table containing YT viewership data at daily talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Datasumm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - summ Models :           NA
    - Final Model :           summ_daily_talent_yt_viewership_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs summ_daily_talent_yt_viewership_table_documentation %}

{% docs intm_daily_talent_yt_split_table_documentation %}

Model Overview :
    -   Intermediate table containing YT viewership data split by talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Datasumm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - summ Models :           NA
    - Final Model :           intm_daily_talent_yt_split_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs intm_daily_talent_yt_split_table_documentation %}

{% docs summ_daily_talent_yt_split_table_documentation %}

Model Overview :
    -   Intermediate table containing YT viewership data split by talent level 

Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Datasumm :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        Talent Equity
    - Owner:        Sourish Chakraborty
    - Version Logs:
        Version      Date            Author               Request
        1.0          02/22/2020     schakrab              PSTA-2456
    

Model Pipeline Overview :
    - Pipeline  :             NA
    - summ Models :           NA
    - Final Model :           summ_daily_talent_yt_split_table
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA


{% enddocs summ_daily_talent_yt_split_table_documentation %}