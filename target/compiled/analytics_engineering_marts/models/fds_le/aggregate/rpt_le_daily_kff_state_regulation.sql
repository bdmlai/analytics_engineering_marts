
with __dbt__CTE__intm_le_kff_state_regulation as (

select (current_date - 1) as state_regulation_update_date, province_state as state,
replace(replace(replace(status_of_reopening,'-',''),';',' '),'>','Greater than ') as status_of_reopening,
replace(replace(replace(stay_at_home_order,'-',''),';',' '),'>','Greater than ') as stay_at_home_order,
replace(replace(replace(mandatory_quarantine_for_travelers,'-',''),';',' '),'>','Greater than ') as mandatory_quarantine_for_travelers,
replace(replace(replace(non_essential_business_closures,'-',''),';',' '),'>','Greater than ') as non_essential_business_closures,
replace(replace(replace(large_gatherings_ban,'-',''),';',' '),'>','Greater than ') as large_gatherings_ban,
replace(replace(replace(restaurant_limits,'-',''),';',' '),'>','Greater than ') as restaurant_limits,
replace(replace(replace(bar_closures,'-',''),';',' '),'>','Greater than ') as bar_closures,
replace(replace(replace(face_covering_requirement,'-',''),';',' '),'>','Greater than ') as face_covering_requirement,
replace(replace(replace(primary_election_postponement,'-',''),';',' '),'>','Greater than ') as primary_election_postponement,
replace(replace(replace(emergency_declaration,'-',''),';',' '),'>','Greater than ') as emergency_declaration
from "starschema_aws_us_east_va_covid19_by_starschema_dm"."public"."kff_us_state_mitigations"
)select a.state_regulation_update_date, a.state, a.status_of_reopening::varchar(255) as status_of_reopening, 
a.stay_at_home_order::varchar(255) as stay_at_home_order, 
a.mandatory_quarantine_for_travelers::varchar(255) as mandatory_quarantine_for_travelers, 
a.non_essential_business_closures::varchar(255) as non_essential_business_closures, 
a.large_gatherings_ban::varchar(255) as large_gatherings_ban,
case 
when b.lkp_description is null then trim(a.large_gatherings_ban)::varchar(500)
else trim(b.lkp_description) 
end as large_gatherings_ban_bucket_group,
a.restaurant_limits::varchar(255) as restaurant_limits, a.bar_closures::varchar(255) as bar_closures, 
a.face_covering_requirement::varchar(255) as face_covering_requirement,
case 
when c.lkp_description is null then trim(a.face_covering_requirement)::varchar(500)
else trim(c.lkp_description)
end as face_covering_requirement_bucket_group,
a.primary_election_postponement::varchar(255) as primary_election_postponement, 
a.emergency_declaration::varchar(255) as emergency_declaration,
('DBT_' || TO_CHAR(((convert_timezone ('UTC', getdate()))::timestamp),'YYYY_MM_DD_HH24_MI_SS') || '_5A')::varchar(255) as etl_batch_id, 
'SVC_PROD_BI_DBT_USER' as etl_insert_user_id, ((convert_timezone ('UTC', getdate()))::timestamp) as etl_insert_rec_dttm, 
null::varchar(255) as etl_update_user_id, cast(null as timestamp) as etl_update_rec_dttm
from
(select * from (select *,row_number()over(partition by state, state_regulation_update_date order by status_of_reopening desc) as rank1 
from __dbt__CTE__intm_le_kff_state_regulation) 
where rank1 = 1) a
left join 
(select lkp_code, lkp_description 
from "dev_entdwdb"."cdm"."lookup_table"
where lkp_name = 'Regulations_LG_Buckets') b on trim(lower(a.large_gatherings_ban)) = trim(lower(b.lkp_code))
left join (select lkp_code, lkp_description 
from "dev_entdwdb"."cdm"."lookup_table"
where lkp_name = 'Regulations_FC_Buckets') c on trim(lower(a.face_covering_requirement)) = trim(lower(c.lkp_code))