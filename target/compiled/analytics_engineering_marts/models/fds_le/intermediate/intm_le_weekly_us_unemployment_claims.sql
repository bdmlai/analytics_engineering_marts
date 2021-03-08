
with __dbt__CTE__intm_le_weekly_us_unemployment_data as (

select *, row_number() over (partition by state order by filed_week_ended desc) as rank
from
(select * from 
(select *, extract(week from filed_week_ended::date) as week_num, extract(year from filed_week_ended::date) as year_num,
row_number() over (partition by state, filed_week_ended order by as_on_date desc) as rank_1  
from "prod_entdwdb"."udl_cp"."le_weekly_us_unemployment_claims_details")
where rank_1 = 1)
)select a.*, b.initial_claims as initial_claims_14day_prior, b.continued_claims as continued_claims_14day_prior
from __dbt__CTE__intm_le_weekly_us_unemployment_data a 
left join __dbt__CTE__intm_le_weekly_us_unemployment_data b on a.state = b.state and a.rank = b.rank - 2