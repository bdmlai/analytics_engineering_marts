{{
  config({
		"materialized": 'ephemeral'
  })
}}
select
    event_id,
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
    {{ref('intm_le_customer_rpt_tbl')}} a
join
    {{source('cdm','dim_date')}} b
on
    a.event_date = b.full_date
where
    event_name in ('clashofchampions',
                   'eliminationchamber',
                   'hellinacell',
                   'payback',
                   'raw',
                   'royalrumble',
                   'smackdown',
                   'summerslam',
                   'survivorseries',
                   'tlc')