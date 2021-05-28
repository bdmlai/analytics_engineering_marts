{% docs base_cpg_daily_prod_plcmnt_split_stage1_documentation %}
Model Overview :
    Objective -  
       To track the visits, revenue & transaction in WWE Shop site over the next 20 mins after a talent appears on screen wearing a merch 
    Descritpion-
       "Script + Database + Analysis to optimize Product Placements for the CPG team. This means looking at the time of product placement and observing sales over the next 20 minutes.
       Create script to ingest and organize data from the litelogs related to organic advertisements and product placement (e.g. talent wearing merch or talking about a product)
       Perform analysis to optimize by time, talent, product category, etc."
        
    For detailed metrics, please go through Pipeline Overview belowx. For column description, please go through  Columns section  
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             DBT CLI
    - Base Models : 
                            - base_cpg_daily_prod_plcmnt_split_stage1
                              base_cpg_daily_prod_plcmnt_visits
                              base_cpg_daily_prod_plcmnt_items
    - Intermediate Models : 
                            - intm_cpg_daily_prod_plcmnt_split_stage2 
                              intm_cpg_daily_prod_plcmnt_split_stage3
                              intm_cpg_daily_prod_plcmnt_split_stage4
                              intm_cpg_daily_prod_plcmnt_split_stage4_update 
                                                                     
    - Summary Models : 
                            - summ_cpg_prod_plcmnt_minute_program_stage1
                              summ_cpg_prod_plcmnt_minute_program_stage2
                              summ_cpg_prod_plcmnt_minute_program_final
                            
    - Final Models : 
                            - final_cpg_daily_prod_plcmnt_output 
                              rpt_cpg_daily_prod_plcmnt_output_revenue

    - Deployment Environment: COE Analytics Engineering Deployment
    - Schedule Name:          
    - Schedule Frequency :    Daily
{% enddocs base_cpg_daily_prod_plcmnt_split_stage1_documentation %}

{% docs base_cpg_daily_prod_plcmnt_visits_documentation %}
Model Overview:
    -   This model keeps a check on visits.
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs base_cpg_daily_prod_plcmnt_visits_documentation %}

{% docs base_cpg_daily_prod_plcmnt_items_documentation %}
Model Overview:
    -   This model looks on distinct items and descriptions.
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs base_cpg_daily_prod_plcmnt_items_documentation %}

{% docs intm_cpg_daily_prod_plcmnt_split_stage2_documentation %}
Model Overview:
    -   This model Splitparts the comments sub part by ","
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs intm_cpg_daily_prod_plcmnt_split_stage2_documentation %}

{% docs intm_cpg_daily_prod_plcmnt_split_stage3_documentation %}
Model Overview:
    -   This model Splitparts the comments sub part by "!"
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs intm_cpg_daily_prod_plcmnt_split_stage3_documentation %}

{% docs intm_cpg_daily_prod_plcmnt_split_stage4_documentation %}
Model Overview:
    -   This model Splitparts the comments sub part by "and"
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs intm_cpg_daily_prod_plcmnt_split_stage4_documentation %}

{% docs intm_cpg_daily_prod_plcmnt_split_stage4_update_documentation %}
Model Overview:
    -   This model Updates the comments by replacing "wearing merch" with blank
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs intm_cpg_daily_prod_plcmnt_split_stage4_update_documentation %}

{% docs summ_cpg_daily_prod_plcmnt_minute_program_stage1_documentation %}
Model Overview:
    -   This model Calculates the program start time from minimum of inpoint.
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs summ_cpg_daily_prod_plcmnt_minute_program_stage1_documentation %}

{% docs summ_cpg_daily_prod_plcmnt_minute_program_stage2_documentation %}
Model Overview:
    -   This model Joins the min_pgm model for 20 times for creating 20 mins window.
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs summ_cpg_daily_prod_plcmnt_minute_program_stage2_documentation %}

{% docs summ_cpg_daily_prod_plcmnt_minute_program_final_documentation %}
Model Overview:
    -   This model Ranks the rows for distinct logentryguid
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs summ_cpg_daily_prod_plcmnt_minute_program_final_documentation %}

{% docs final_cpg_daily_prod_plcmnt_output_documentation %}
Model Overview:
    -   This model contains final data for product placement analysis
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs final_cpg_daily_prod_plcmnt_output_documentation %}

{% docs rpt_cpg_daily_prod_plcmnt_output_revenue_documentation %}
Model Overview:
    -   This model appends visits, revenue and transactions to final
Model Specification :
    - Tier :        Silver
    - Warehouse :   ANALYTICS_WH
    - Database :    ANALYTICS_WORKSPACE
    - Schema :      CREATIVE
    - Org :         Center of Excellence
    - Team :        CPG
    - Owner:        Sushree Nayak & Simranjit Singh
    - Version Logs:
        Version      Date            Author               Request
        1.0          03/08/2020     Schakraborty          ADVA-214
Model Pipeline Overview :
    - Pipeline  :             NA
    - Base Models :           NA
    - Final Model :          rpt_cpg_daily_prod_plcmnt_output_revenue
    - Deployment Environment: NA
    - Schedule Name:          NA
    - Schedule Frequency :    NA
{% enddocs rpt_cpg_daily_prod_plcmnt_output_revenue_documentation %}