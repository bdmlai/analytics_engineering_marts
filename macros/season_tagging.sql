/*
*************************************************************************************************************************************************
   macro name   : season_tagging
   Schema	     : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Macro which takes attribute,column_name and condition as input and returns the
                 tagged attribute if column value lies between the specified range
   
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% macro season_tagging(attribute,column_name,condition)%}


case 
{% for p,q,r in condition %}
when {{column_name}} between '{{p}}' and '{{q}}' then '{{r}}'
{% endfor %}
else 'NA'
end as att_{{attribute}}


{% endmacro %}