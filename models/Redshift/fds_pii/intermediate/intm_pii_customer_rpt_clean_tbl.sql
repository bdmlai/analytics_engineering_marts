{{
  config({
		"materialized": 'ephemeral'
  })
}}
select
    event_id,
    case
        when c.lkp_description is null
        then a.event_name
        else c.lkp_description
    end as event_name,
    event_date,
    reg_date,
    user_id,
    email,
    country,
    region,
    virtual_seat_attended,
    as_on_date,
    etl_batch_id_source
from
    {{ref('intm_pii_customer_rpt_tbl')}} a
join
    {{source('cdm','dim_date')}} b
on
    a.event_date = b.full_date
left join
	(select distinct lkp_code, lkp_description
	 from {{source('cdm','lookup_table')}}
	 where lkp_name = 'Thunderdome_Formatted_Event_Names') c
on
	a.event_name = c.lkp_code
where
    event_id not in 
	(select trim(lower(lkp_code)) 
	 from {{source('cdm','lookup_table')}}
	 where lkp_name = 'Thunderdome_Excluded_Events')