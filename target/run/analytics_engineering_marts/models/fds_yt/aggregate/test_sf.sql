

      create or replace transient table DEV_ENTDWDB.fds_cp.test_sf  as
      (
select * from cdm.dim_region_country
      );
    