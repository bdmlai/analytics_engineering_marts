{{
  config({
		"materialized": 'ephemeral'
  })
}}
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
from {{source('prod_entdwdb.public','kff_us_state_mitigations')}}