{{
  config({
		"materialized": 'ephemeral'
  })
}}
SELECT
    *
FROM
    (
        SELECT
            *,
            row_number() over(partition BY iso3166_2, fips, date ORDER BY cases DESC) AS
            rank_covid_dedupe
        FROM
            (
                SELECT
                    date,
                    state,
                    CASE
                        WHEN trim(LOWER(county)) = 'unknown'
                        THEN 'Statewide Unallocated'
                        ELSE county
                    END AS county,
                    CASE
                        WHEN trim(LOWER(county)) = 'new york city'
                        AND trim(LOWER(state)) = 'new york'
                        THEN '36061'
                        WHEN trim(LOWER(county)) IN ('unknown',
                                                     'statewide unallocated')
                        THEN '0'
                        ELSE fips
                    END AS fips,
                    cases,
                    deaths,
                    iso3166_1,
                    iso3166_2
                FROM
                    {{source('prod_entdwdb.public','nyt_us_covid19')}}
                WHERE
                    trim(LOWER(state)) NOT IN ('guam',
                                               'northern mariana islands',
                                               'puerto rico',
                                               'virgin islands') ))
WHERE
    rank_covid_dedupe = 1 