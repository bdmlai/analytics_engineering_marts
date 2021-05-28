/*
*************************************************************************************************************************************************
   TableName   : rpt_cpg_daily_prod_plcmnt_output_revenue
   Schema	   : CREATIVE
   Contributor : Sushree Nayak & Simranjit Singh
   Description : To append revenue, visits and transactions to final model
   Version      Date            Author               Request
   1.0          03/15/2021      schakraborty         ADVA-214
*************************************************************************************************************************************************
*/
{{ config(materialized = 'table',schema='fds_cpg',
            enabled = true, 
                tags = ['cpg','prod_plcmnt','airdate','adj_time'],
                    post_hook = "grant select on {{ this }} to DA_MIGNAL_USER_ROLE;
                                drop table dt_stage.base_cpg_daily_prod_plcmnt_visits;
                                drop table dt_stage.base_cpg_daily_prod_plcmnt_items;
                                drop table dt_stage.base_cpg_daily_prod_plcmnt_split_stage1;
                                drop table dt_stage.intm_cpg_daily_prod_plcmnt_split_stage2;
                                drop table dt_stage.intm_cpg_daily_prod_plcmnt_split_stage3;
                                drop table dt_stage.intm_cpg_daily_prod_plcmnt_split_stage4;
                                drop table dt_stage.intm_cpg_daily_prod_plcmnt_split_stage4_update;
                                drop table dt_stage.summ_cpg_daily_prod_plcmnt_minute_program_final;
                                drop table dt_stage.summ_cpg_daily_prod_plcmnt_minute_program_stage1;
                                drop table dt_stage.summ_cpg_daily_prod_plcmnt_minute_program_stage2;
                                drop table dt_stage.final_cpg_daily_prod_plcmnt_output; "
 ) }}

with source_data AS 
    (SELECT a.airdate,
         a.dt_time,
         a.logentryguid,
         a.show,
         a.typeofshow,
         a.segmenttype,
         a.inpoint,
         a.talent_listed,
         a.talent_identified,
         a.min_pgm_val,
         a.adj_time,
         sum(d.vis) AS vis,
         sum(d.trans) AS trans,
         sum(d.rev) AS rev
    FROM 
        (SELECT airdate,
         dt_time,
         logentryguid,
         show,
         typeofshow,
         segmenttype,
         inpoint,
         talent_listed,
         talent_identified,
         min_pgm_val,
         adj_time
        FROM {{ref('final_cpg_daily_prod_plcmnt_output')}}) a
        LEFT JOIN 
            (SELECT date,
         substring(hits_time_est,
         1,
         16) AS hits_time_est,
         src_talent_description,
         count(distinct visits) AS vis,
         sum(rev) AS rev,
         count(distinct trans) AS trans
            FROM {{ref('base_cpg_daily_prod_plcmnt_visits')}} b
            LEFT JOIN {{ref('base_cpg_daily_prod_plcmnt_items')}} c
                ON b.productSku=c.src_item_id
            WHERE src_talent_description is NOT null
            GROUP BY  1,2,3) d
                ON a.dt_time=d.hits_time_est
                    AND trim(lower(a.talent_identified))=trim(lower(d.src_talent_description))
            GROUP BY  1,2,3,4,5,6,7,8,9,10,11 )
        SELECT airdate,
         dt_time,
         logentryguid,
         show,
         typeofshow,
         segmenttype,
         inpoint,
         talent_listed,
         talent_identified,
         min_pgm_val,
         adj_time,
         vis,
         trans,
         rev,
		 'DBT_'||TO_CHAR(SYSDATE(),'YYYY_MM_DD_HH_MI_SS')||'_Prod_plc' AS etl_batch_id , 'bi_dbt_user_prd' AS etl_insert_user_id , SYSDATE() AS etl_insert_rec_dttm , NULL AS etl_update_user_id , cast(null AS timestamp) AS etl_update_rec_dttm
    FROM source_data