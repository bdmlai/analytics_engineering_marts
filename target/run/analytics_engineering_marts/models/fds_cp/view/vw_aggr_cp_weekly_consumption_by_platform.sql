

  create view "entdwdb"."dwh_read_write"."vw_aggr_cp_weekly_consumption_by_platform__dbt_tmp" as (
    

select * from "entdwdb"."dwh_read_write"."aggr_cp_weekly_consumption_by_platform"
  ) ;
