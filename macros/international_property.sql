/*
*************************************************************************************************************************************************
   Macro name   : international_property
   Schema	    : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Macro which takes  column_name,condition it has to satisfy,constrains(in,not in)
                 and flag_number as input and returns the flag value as 1 
   
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/


{% macro international_property(column_name,condition,constrain,flag_number)%}

case 
when {{column_name}} {{constrain}} {{condition}} then 1
else 0
end as {{column_name}}_flag_{{flag_number}}

{% endmacro %}