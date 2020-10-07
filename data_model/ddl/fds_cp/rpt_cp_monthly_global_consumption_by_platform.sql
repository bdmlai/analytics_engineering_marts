CREATE TABLE
    fds_cp.rpt_cp_monthly_global_consumption_by_platform
    (
        platform CHARACTER VARYING(16383) ENCODE LZO,
        type CHARACTER VARYING(16383) ENCODE LZO,
        type2 CHARACTER VARYING(16383) ENCODE LZO,
        region CHARACTER VARYING(16383) ENCODE LZO,
        country CHARACTER VARYING(16383) ENCODE LZO,
        MONTH DATE ENCODE AZ64,
        views DOUBLE PRECISION,
        hours_watched DOUBLE PRECISION,
        prev_month_views DOUBLE PRECISION,
        prev_month_hours DOUBLE PRECISION,
        ytd_views DOUBLE PRECISION,
        ytd_hours_watched DOUBLE PRECISION,
        prev_year_views DOUBLE PRECISION,
        prev_year_hours DOUBLE PRECISION,
        etl_batch_id INTEGER ENCODE AZ64,
        etl_insert_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_update_user_id CHARACTER VARYING(1) ENCODE LZO,
        etl_update_rec_dttm TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
        etl_insert_user_id CHARACTER VARYING(15) ENCODE LZO
    );
	
	
comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.platform
"stores the name of cross platform";

comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.monday_date
"represents the start of the week for measurement period Monday to Sunday";

comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.views
"indicates the total views for the week for each platform";

comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.minutes_watched
"indicates the total minutes watched for the week for each platform";	

comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.prev_views
"indicates the total minutes watched for the week for each platform";	


comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.prev_mins
"indicates the total views for previous week for each platform"

comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.weekly_per_change_views
"indicates the total minutes watched for previous week for each platform";

comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.weekly_per_change_mins
"gives the week over week change in total views for each platform";

comment on column fds_cp.rpt_cp_monthly_global_consumption_by_platform.monday_date
"gives the week over week change in total minutes watched for each platform";	