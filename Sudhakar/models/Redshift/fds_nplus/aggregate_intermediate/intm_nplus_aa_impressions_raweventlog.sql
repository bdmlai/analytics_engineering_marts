{{
  config({
		"materialized": 'ephemeral'
  })
}}

     
            
         
            SELECT * FROM {{ref('intm_nplus_aa_impressions_encompass_nxt')}}
            UNION ALL
            SELECT * FROM {{ref('intm_nplus_aa_impressions_encompass_205new')}}
            UNION ALL
            SELECT * FROM {{ref('intm_nplus_aa_impressions_encompass_205old')}}
            UNION ALL
            SELECT * FROM {{ref('intm_nplus_aa_impressions_encompass_205fb')}}
            UNION ALL
            SELECT * FROM {{ref('intm_nplus_aa_impressions_raweventlog_ppv')}}
