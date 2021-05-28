/*
*************************************************************************************************************************************************
   Macro name   : series_ilike
   Schema	    : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Macro which takes  column_name,condition it has to satisfy,
                 and flag_number as input and returns the flag value 
   
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% macro series_ilike(column_name,condition,flag_number)%}


case 
when {{column_name}} ilike '{{condition}}' then 1
else 0
end as {{column_name}}_ilike_{{flag_number}}

{% endmacro %}