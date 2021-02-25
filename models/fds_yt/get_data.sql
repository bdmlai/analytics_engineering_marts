{% macro get_data() %}
    {% set query %}

	with tab1 as
	(
	select group_id, REPLACE(sql_header,'|||||',CHR(10)) || REPLACE(sql_logic,'|||||',CHR(10)) || REPLACE(sql_footer,'|||||',CHR(10)) as sql_logic from (
	with dataset_calculate_vars as
	(
			select var_id,group_id, ('|||||, ' || coalesce(sql_logic,'null') || ' as ' || var_nm) as sql_logic
			from fds_pii.fan_var_config_bi
			where list_nm='master' and active_ind=1  and
									(case when 'incremental'='incremental' then hist_update_ind=1 when 'incremental'='history' then trunc(hist_update_date)=current_date end)
			order by var_id
	),
	dataset_query as
	(select 1 as type, group_id, listagg(sql_logic) within group (order by var_id ) as sql_logic
	,(case when 'incremental'='incremental' then '=' when 'incremental'='history' then '<=' end) as fetch_condition from dataset_calculate_vars group by 1,2)
	,
	dataset_final as
	(
	select type, group_id,

	(case when group_id<>1 then '|||||' || 'select distinct dim_fan_email.src_fan_id,dim_fan_email.fan_email_id as email '
	else  '|||||' || 'select distinct dim_fan_email.src_fan_id '   end)as sql_header
	, sql_logic
	, ('|||||' || ','||'current_date'|| ' as as_on_date'||' from (select * from fds_pii.dim_fan_email where active_record_ind = 1) dim_fan_email;'|| '|||||') as sql_footer
	from dataset_query
	)
	select group_id as group_id
	, (case when group_id is null then 'select current_date' else sql_header end) as sql_header
	, (case when group_id is null then null else sql_logic end) as sql_logic
	, (case when group_id is null then null else sql_footer end) as sql_footer
	from dataset_final a
	))
	select sql_logic from tab1

    {% endset %}

    {% set results = run_query(query) %}
    {# execute is a Jinja variable that returns True when dbt is in "execute" mode i.e. True when running dbt run but False during dbt compile. #}
    {% if execute %}
    {% set results_list = results.rows %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}

{% endmacro %}


