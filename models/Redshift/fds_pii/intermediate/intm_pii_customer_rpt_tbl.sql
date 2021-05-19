{{
  config({
		"materialized": 'ephemeral'
  })
}}
select
    a.event_id,
	case
		when regexp_count(right((lower(trim(a.event_id))), 8), '[0-9]{8}') = 1 
		then (substring(lower(trim(a.event_id)), (len(trim(a.event_id)) - 3), 4) || '-' ||
			  substring(lower(trim(a.event_id)), (len(trim(a.event_id)) - 7), 2) || '-' ||
	          substring(lower(trim(a.event_id)), (len(trim(a.event_id)) - 5), 2))
		when regexp_count(left((lower(trim(a.event_id))), 8), '[0-9]{8}') = 1 
		then (substring(lower(trim(a.event_id)), 1, 4) || '-' ||
			  substring(lower(trim(a.event_id)), 5, 2) || '-' ||
	          substring(lower(trim(a.event_id)), 7, 2))
		else '0001-01-01'
	end                                                          as event_date,	
    substring(trim(a.created_timestamp), 1, 10)                  as reg_date,
	case
		when regexp_count(right((lower(trim(a.event_id))), 8), '[0-9]{8}') = 1 
		then left((lower(trim(a.event_id))), (len(trim(a.event_id)) - 8))
		when regexp_count(left((lower(trim(a.event_id))), 8), '[0-9]{8}') = 1
		then right((lower(trim(a.event_id))), (len(trim(a.event_id)) - 8))
		else a.event_id
	end as event_name,
    a.user_id,
    a.email,
    a.country,
    case
        when lower(trim(a.country)) = 'united states'
        then 'domestic'
        else 'international'
    end as region,
    case
	    when a.virtual_seat_attended = true
		then 'true'
		else 'false'
	end as virtual_seat_attended,
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
    a.created_timestamp is not null and
    lower(trim(a.event_id))not like '%test%' and
	lower(trim(a.event_id))not like '%mock%' and
	lower(trim(a.event_id))not like '%integration%' and
	lower(trim(a.event_id))not like '%duplicate%' and
	lower(trim(a.event_id))not like '%not-%' and
	lower(trim(a.event_id))not like '%maybe-%'