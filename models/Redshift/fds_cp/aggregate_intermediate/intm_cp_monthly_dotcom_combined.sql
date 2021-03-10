
{{
  config({
		"materialized": 'ephemeral'
  })
}}

select date as month,country,
'.COM Page Views' as metric,
'NA' as page,
page_views as values,'.COM' as platform
from {{ref('intm_cp_monthly_dotcom')}}

union all

select date as month,country,
'.COM Views' as metric,
'NA' as page,
video_views as values,'.COM' as platform
from {{ref('intm_cp_monthly_dotcom')}}

union all

select date as month,country,
'.COM Hours' as metric,
'NA' as page,
hours_watched as values,'.COM' as platform
from {{ref('intm_cp_monthly_dotcom')}}

union all

select date as month,country,
'.COM Monthly Total Unique Cookies' as metric,
'NA' as page,
unique_visitors as values,'.COM' as platform
from {{ref('intm_cp_monthly_dotcom')}}

union all

select date as month,country,
'.COM Visits' as metric,
'NA' as page,
visits as values,'.COM' as platform
from {{ref('intm_cp_monthly_dotcom')}}