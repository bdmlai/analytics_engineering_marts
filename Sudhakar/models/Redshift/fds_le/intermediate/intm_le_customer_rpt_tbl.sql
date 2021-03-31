{{
  config({
		"materialized": 'ephemeral'
  })
}}
select
    a.event_id,
    (substring(lower(trim(a.event_id)), (len(trim(a.event_id)) - 3), 4) || '-' ||
	 substring(lower(trim(a.event_id)), (len(trim(a.event_id)) - 7), 2) || '-' ||
	 substring(lower(trim(a.event_id)), (len(trim(a.event_id)) - 5), 2))
	                                                             as event_date,	
    substring(trim(a.created_timestamp), 1, 10)                  as reg_date,
    left((lower(trim(a.event_id))), (len(trim(a.event_id)) - 8)) as event_name,
    a.user_id,
    a.email,
    a.country,
    case
        when lower(trim(a.country)) = 'united states'
        then 'domestic'
        else 'international'
    end as region,
    a.virtual_seat_attended,
    a.as_on_date,
    a.etl_batch_id as etl_batch_id_source
from
    {{source('udl_nplus','raw_famousgroup_customer_rpt_tbl_extended')}} a
join
    (
        select
            event_id,
            max(as_on_date) as as_on_date
        from
           {{source('udl_nplus','raw_famousgroup_customer_rpt_tbl_extended')}}
        group by
            1 ) b
on
    a.event_id = b.event_id
and a.as_on_date = b.as_on_date
where
    a.created_timestamp is not null