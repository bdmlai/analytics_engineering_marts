


{{
  config({
		"materialized": 'ephemeral'
  })
}}



SELECT
    time_key,
    Item_key,
    event_key,
    venue_key,
    quantity_shipped,
    quantity_adjustment ,
    quantity_returned,
    compelements ,
    net_units_sold ,
    Selling_price,
    total_revenue,
    complement_revenue
FROM
    {{ref('intm_cpg_venue_noncups_dataset')}}
UNION ALL
SELECT
    time_key,
    Item_key,
    event_key,
    venue_key,
    quantity_shipped,
    quantity_adjustment ,
    quantity_returned,
    compelements ,
    net_units_sold ,
    Selling_price,
    total_revenue,
    complement_revenue
FROM
    {{ref('intm_cpg_venue_cups_dataset')}}