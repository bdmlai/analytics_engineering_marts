
select *,convert_timezone('AMERICA/NEW_YORK', sysdate) as etl_insert_dtmm  from #final_table_up