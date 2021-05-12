{{ config({ "materialized": 'ephemeral' }) }}
SELECT *
FROM {{ref('intm_nplus_aa_impressions_encompass_nxt')}}
UNION
SELECT *
FROM {{ref('intm_nplus_aa_impressions_encompass_205new')}}
UNION
SELECT *
FROM {{ref('intm_nplus_aa_impressions_encompass_205old')}}
UNION
SELECT *
FROM {{ref('intm_nplus_aa_impressions_encompass_205fb')}}
UNION
SELECT *
FROM {{ref('intm_nplus_aa_impressions_raweventlog_ppv')}} 