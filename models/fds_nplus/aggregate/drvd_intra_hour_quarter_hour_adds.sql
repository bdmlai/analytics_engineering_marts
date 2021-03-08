 {{
  config({
	"schema": 'fds_nplus',	
	"materialized": 'incremental',
	"pre-hook":["delete  from fds_nplus.drvd_intra_hour_quarter_hour_adds where date = (case when extract(hour from convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp as timestamp)))=0
				and  extract(minute from convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp as timestamp)))<15
				then trunc(convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp -1 as timestamp))) else
				trunc(convert_timezone('AMERICA/NEW_YORK', sysdate)) end );"]})}}
with  #derived as 
	(Select date, hour,minute,
	case
	when minute>=0 and minute<=15 then 'Quarter Hour 1'
	when minute>=16 and minute<=30 then 'Quarter Hour 2'
	when minute>=31 and minute<=45 then 'Quarter Hour 3'
	else 'Quarter Hour 4' end as Quarter_Hour,
	time_stamp,payload_data_payment_provider,
	count(distinct case when flag = 'Paid' then customerid else null end) as  paid_adds,
	count(distinct case when flag = 'Trial' then customerid else null end) as  trial_adds from
	(Select customerid,payload_data_payment_provider,
	trunc(CONVERT_TIMEZONE('AMERICA/NEW_YORK', cast(ts as timestamp))) as date,
	min(extract(hour from CONVERT_TIMEZONE('AMERICA/NEW_YORK', cast(ts as timestamp)))) as hour,
	min(extract(minute from CONVERT_TIMEZONE('AMERICA/NEW_YORK', cast(ts as timestamp)))) as minute,
	min(CONVERT_TIMEZONE('AMERICA/NEW_YORK', cast(ts as timestamp))) as time_stamp,
	case    when payload_data_voucher_code is not null and payload_data_voucher = true 
	and payload_data_price_with_tax_amount =0 and payload_data_renewal = 'false' then 'Trial'          
        	when payload_data_is_trial='true' and payload_data_price_with_tax_amount =0 then 'Trial' 
	when payload_data_is_trial='false' then 'Paid' end as flag 
	from {{source('udl_nplus','stg_dice_stream_flattened')}}
	where payload_data_ta in ('SUCCESSFUL_PURCHASE')
	and (payload_data_voucher_code is null or payload_data_voucher_code!='WWE Network VIP')
	and (payload_data_renewal !='true' or payload_data_renewal is null)
	and trunc(CONVERT_TIMEZONE('AMERICA/NEW_YORK', cast(ts as timestamp))) = (
	case when extract(hour from convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp as timestamp)))=0
	and  extract(minute from convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp as timestamp)))<15
	then trunc(convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp -1 as timestamp))) else
	trunc(convert_timezone('AMERICA/NEW_YORK', sysdate)) end ) group by 1,2,3,7)
group by 1,2,3,4,5,6)
select date, hour, quarter_hour, sum(paid_adds) paid_adds, sum(trial_adds) trial_adds,payload_data_payment_provider as payment_method,
(case when extract(hour from convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp as timestamp)))=0
	and  extract(minute from convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp as timestamp)))<15
	then convert_timezone('AMERICA/NEW_YORK', cast(current_timestamp -1 as timestamp)) else
	convert_timezone('AMERICA/NEW_YORK', sysdate) end ) etl_insert_rec_dttm from #derived group by 1,2,3,6