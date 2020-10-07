

  create view "entdwdb"."fds_nplus"."vw_rpt_network_ppv_liveplusvod__dbt_tmp" as (
    
select * from "entdwdb"."fds_nplus"."rpt_network_ppv_liveplusvod"
  ) ;
