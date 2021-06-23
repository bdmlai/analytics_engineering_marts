
{{
  config({
		"materialized": 'ephemeral'
  })
}}



                SELECT
                    *
                FROM
                    {{source('fds_cpg','dim_cpg_item')}}
                WHERE
                    dim_business_unit_id IN
                    (
                        SELECT
                            dim_business_unit_id
                        FROM
                            {{source('fds_cpg','dim_cpg_business_unit')}}
                        WHERE
                            src_business_unit_id='W03')
                AND UPPER(active_flag) = 'Y'