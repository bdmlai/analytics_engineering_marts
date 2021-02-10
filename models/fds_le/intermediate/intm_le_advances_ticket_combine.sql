{{
  config({
		"materialized": 'ephemeral'
  })
}}

select * from (
select * from {{ref('intm_le_advances_ticket')}}
union all
select * from {{ref('intm_le_advances_ticket_ppv')}}
)