/*
*************************************************************************************************************************************************
   Macro name   : property_tagging
   Schema	    : CONTENT
   Contributor : B.V.Sai Praveen Chakravarthy & Raghava Bavisetty
   Description : Macro which takes attribute,2 columns,their conditions,2 constrains(not/"")
                 and connector(and/or) as input and returns the flag value 
                 if both columns satisfy the specified conditions
   
   Version      Date             Author               Request
   1.0          03/15/2021       pchakrav             ADVA-297
*************************************************************************************************************************************************
*/

{% macro property_tagging(attribute,column_1,condition_1,column_2,condition_2,
                    constrain_1,connector,constrain_2)%}
case
when
(
{%for i in condition_1%}
{{column_1}} {{constrain_1}} like '%{{i}}%' {% if not loop.last %}or{% endif %} {% endfor %}
) 

{{connector}} 

{% if condition_2 !='' %}
(
{%for j in condition_2%}
{{column_2}} {{constrain_2}} like '%{{j}}%' {% if not loop.last %}or{% endif %}{% endfor %} 
)
{% endif %}
 then 1
else 0 end as {{attribute}}_{{constrain_1}}_{{connector}}_{{constrain_2}}_flag
{% endmacro %}
