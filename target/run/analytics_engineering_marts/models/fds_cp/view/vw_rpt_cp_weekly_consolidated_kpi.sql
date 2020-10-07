

  create view "entdwdb"."fds_cp"."vw_rpt_cp_weekly_consolidated_kpi__dbt_tmp" as (
    
select * from "entdwdb"."fds_nplus"."rpt_cp_weekly_consolidated_kpi"
union all
select * from "entdwdb"."fds_le"."rpt_le_weekly_consolidated_kpi"
  ) ;
