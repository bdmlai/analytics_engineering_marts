{{
  config({
		"schema": 'fds_cpg',
		"materialized": 'ephemeral',"tags": 'rpt_cpg_weekly_consolidated_kpi'
  })
}}

        select v.date as date, 
        z.dim_shop_site_id, 
        sum(case when z.date = v.date then sales else 0 end) sales
        from 
                (select cast(cast(a.date_key as varchar(10)) as date) as date, 
                a.dim_shop_site_id, 
                b.src_category_description, 
                b.src_major_category_description, 
                sum(demand_sales_$) as sales
                from {{source('fds_cpg','aggr_cpg_daily_sales')}} a 
                left join {{source('fds_cpg','dim_cpg_item')}} b 
                on a.dim_item_id = b.dim_item_id 
                where cast(cast(a.date_key as varchar(10)) as date)>= dateadd('day', -1826, current_date)
                group by 1,2,3,4
                )z,

                (select distinct cast(cast(a.date_key as varchar(10)) as date) date 
                from {{source('fds_cpg','aggr_cpg_daily_sales')}} a 
                where
                cast(cast(a.date_key as varchar(10)) as date)>= dateadd('day', -1461, current_date)) v
                where
                z.date >= add_months(v.date, -12) and z.date <= v.date 
                group by 1,2