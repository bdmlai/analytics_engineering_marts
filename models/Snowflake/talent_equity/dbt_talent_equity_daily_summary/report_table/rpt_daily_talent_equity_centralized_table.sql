/*
*************************************************************************************************************************************************
   TableName   : rpt_daily_talent_equity_centralized_table
   Schema      : CREATIVE
   Contributor : Sourish Chakraborty
   Description : This centralised summary table for talents contains EMM, CPG, shows segments and social engagement measures at daily level

   Version      Date            Author               Request
   1.0          02/10/2021      schakrab             PSTA-2456

*************************************************************************************************************************************************
PSTA-3117 05/03/2021 Remove Intermediate tables for Talent Centralized Table
*/

{{ config(materialized = 'table',schema='fds_cpg',
            enabled = true, 
                tags = ['talent_equity','daily',
                        'youtube','centralized table'],
					post_hook=["grant select on {{ this }} to DA_SCHAKRABORTY_USER_ROLE;
					drop table dt_stage.base_daily_talent_absence_table;      
					drop table dt_stage.base_daily_talent_fb_post_table;          
					drop table dt_stage.base_daily_talent_table;
					drop table dt_stage.base_daily_talent_achievement_table;  
					drop table dt_stage.base_daily_talent_fb_vids_table;          
					drop table dt_stage.base_daily_talent_tv_rating_table;
					drop table dt_stage.base_daily_talent_appearance_table;   
					drop table dt_stage.base_daily_talent_heelface_table;         
					drop table dt_stage.base_daily_talent_tw_post_table;
					drop table dt_stage.base_daily_talent_brand_table;        
					drop table dt_stage.base_daily_talent_ig_post_table;          
					drop table dt_stage.base_daily_talent_tw_vids_table;
					drop table dt_stage.base_daily_talent_cpg_shop_table;     
					drop table dt_stage.base_daily_talent_ig_vids_table;          
					drop table dt_stage.base_daily_talent_twitter_mentions_table;
					drop table dt_stage.base_daily_talent_cpg_venue_table;    
					drop table dt_stage.base_daily_talent_sm_followership_table;  
					drop table dt_stage.base_daily_talent_yt_viewership_table;
					drop table dt_stage.intm_daily_talent_achievement_table;        
					drop table dt_stage.intm_daily_talent_fb_vids_table;          
					drop table dt_stage.intm_daily_talent_tw_post_table;
					drop table dt_stage.intm_daily_talent_appearance_table;         
					drop table dt_stage.intm_daily_talent_ig_post_table;          
					drop table dt_stage.intm_daily_talent_tw_vids_table;
					drop table dt_stage.intm_daily_talent_brand_designation_table;  
					drop table dt_stage.intm_daily_talent_ig_vids_table;          
					drop table dt_stage.intm_daily_talent_twitter_mentions_table;
					drop table dt_stage.intm_daily_talent_cpg_table;                
					drop table dt_stage.intm_daily_talent_sm_followership_table;  
					drop table dt_stage.intm_daily_talent_yt_split_table;
					drop table dt_stage.intm_daily_talent_fb_post_table;            
					drop table dt_stage.intm_daily_talent_tv_rating_table;
					drop table dt_stage.summ_daily_talent_yt_split_table;  
					drop table dt_stage.summ_daily_talent_yt_viewership_table;"])}}
with source_data AS 
    (SELECT a.date,
         a.lineage_name,
         a.lineage_wweid,
         a.gender,
         a.entity_type,
         b.brand,
        b.heel_face,
        b.absence,
         c.achievement,
         d.typeofshow,
        d.title,
        d.segmenttype,
        d.matchtype,
        d.role,
        d.duration,
         e.dim_business_unit_id,
        e.src_style_description,
        e.src_category_description,
         f.TOT_FB_POST,
        f.TOT_ENG_FB_POST,
        g.TOT_IG_POST,
        g.TOT_ENG_IG_POST,
        h.TOT_TW_POST,
        h.TOT_ENG_TW_POST,
         i.TOT_FB_VIDS,
        i.TOT_ENG_FB_VIDS,
        i.TOT_MINS_FB_VIDS,
        i.TOT_VIEWS_FB_VIDS,
         j.TOT_IG_VIDS,
        j.TOT_ENG_IG_VIDS,
        j.TOT_MINS_IG_VIDS,
        j.TOT_VIEWS_IG_VIDS,
         k.TOT_TW_VIDS,
        k.TOT_ENG_TW_VIDS,
        k.TOT_MINS_TW_VIDS,
        k.TOT_VIEWS_TW_VIDS,
         l.tv_rating,
        m.fb_fol,
        m.ig_fol,
        m.tw_fol,
        n.mentions,
         o.TOT_YT_VIDS,
        o.TOT_VIEWS,
        o.TOT_MIN_WAT,
        o.TOT_ENG,
         sum(e.demand_units) AS demand_units,
        sum(e.demand_sales) AS demand_sales
    FROM {{ref('base_daily_talent_table')}} a
    LEFT JOIN {{ref('intm_daily_talent_brand_designation_table')}} b
        ON a.lineage_name=b.lineage_name
            AND a.date=b.date
    LEFT JOIN {{ref('intm_daily_talent_achievement_table')}} c
        ON a.lineage_name=c.lineage_name
            AND a.date=c.date
            AND a.entity_type=c.entity_type
    LEFT JOIN {{ref('intm_daily_talent_appearance_table')}} d
        ON a.lineage_name=d.lineage_name
            AND a.date=d.date
            AND a.entity_type=d.entity_type
    LEFT JOIN {{ref('intm_daily_talent_cpg_table')}} e
        ON upper(a.source_string)=upper(e.src_talent_description)
            AND a.date=e.date
    LEFT JOIN {{ref('intm_daily_talent_fb_post_table')}} f
        ON a.lineage_name=f.lineage_name
            AND a.date=f.date
    LEFT JOIN {{ref('intm_daily_talent_ig_post_table')}} g
        ON a.lineage_name=g.lineage_name
            AND a.date=g.date
    LEFT JOIN {{ref('intm_daily_talent_tw_post_table')}} h
        ON a.lineage_name=h.lineage_name
            AND a.date=h.date
    LEFT JOIN {{ref('intm_daily_talent_fb_vids_table')}} i
        ON a.lineage_name=i.lineage_name
            AND a.date=i.date
    LEFT JOIN {{ref('intm_daily_talent_ig_vids_table')}} j
        ON a.lineage_name=j.lineage_name
            AND a.date=j.date
    LEFT JOIN {{ref('intm_daily_talent_tw_vids_table')}} k
        ON a.lineage_name=k.lineage_name
            AND a.date=k.date
    LEFT JOIN {{ref('intm_daily_talent_tv_rating_table')}} l
        ON a.lineage_name=l.lineage_name
            AND a.date=l.date
    LEFT JOIN {{ref('intm_daily_talent_sm_followership_table')}} m
        ON a.lineage_name=m.lineage_name
            AND a.date=m.date
    LEFT JOIN {{ref('intm_daily_talent_twitter_mentions_table')}} n
        ON a.lineage_name=n.lineage_name
            AND a.date=n.date
    LEFT JOIN {{ref('summ_daily_talent_yt_viewership_table')}} o
        ON a.lineage_name=o.lineage_name
            AND a.date=o.date
    GROUP BY  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41, 42,43,44,45 )
SELECT *,
        'DBT_'||TO_CHAR(SYSDATE(),'YYYY_MM_DD_HH_MI_SS')||'_Talent' AS etl_batch_id , 'bi_dbt_user_prd' AS etl_insert_user_id , SYSDATE() AS etl_insert_rec_dttm , NULL AS etl_update_user_id , cast(null AS timestamp) AS etl_update_rec_dttm
FROM source_data 