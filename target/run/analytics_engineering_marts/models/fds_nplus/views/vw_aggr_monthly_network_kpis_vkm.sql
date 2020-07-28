

  create view "entdwdb"."fds_nplus"."vw_aggr_monthly_network_kpis_vkm__dbt_tmp" as (
    
select * from "entdwdb"."fds_nplus"."aggr_monthly_network_kpis_vkm"
  ) ;
