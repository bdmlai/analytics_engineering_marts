{{
  config({
		"materialized": 'ephemeral'
  })
}}

     
            
            SELECT  *  FROM {{ref('intm_nplus_aa_impressions_raweventlog_encompass')}}

