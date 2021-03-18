
select dim_country_id,case when country_name='USA' then 'United States' else country_name end as Country, 
sum(population) as Population from "entdwdb"."cdm"."dim_country_population" group by 1,2