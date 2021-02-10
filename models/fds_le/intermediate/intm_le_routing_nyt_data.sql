{{
  config({
		"materialized": 'ephemeral'
  })
}}
select * from
(select *, row_number() over(partition by iso3166_2, fips, date order by cases desc) as rank_covid_dedupe  
from 
(select date, state,
case 
when trim(lower(county)) = 'unknown' then 'Statewide Unallocated'
else county
end as county,
case 
when trim(lower(county)) = 'new york city' and trim(lower(state)) = 'new york' 
then '36061'
when trim(lower(county)) in ('unknown', 'statewide unallocated')
then '0'
else fips
end as fips,
cases, deaths, iso3166_1, iso3166_2 
from {{source('sf_public','nyt_us_covid19')}}
where trim(lower(state)) not in ('guam','northern mariana islands','puerto rico','virgin islands')
))
where rank_covid_dedupe = 1 