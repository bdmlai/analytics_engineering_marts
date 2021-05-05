{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *,
    row_number() over (partition BY state ORDER BY filed_week_ended DESC) AS rank
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    *,
                    extract(week FROM filed_week_ended::DATE) AS week_num,
                    extract(year FROM filed_week_ended::DATE) AS year_num,
                    row_number() over (partition BY state, filed_week_ended ORDER BY as_on_date
                    DESC) AS rank_1
                FROM
                    {{source('prod_entdwdb.udl_cp','le_weekly_us_unemployment_claims_details')}})
        WHERE
            rank_1 = 1)