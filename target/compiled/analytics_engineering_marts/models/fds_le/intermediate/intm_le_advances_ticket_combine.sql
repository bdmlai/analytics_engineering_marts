

select * from (
select * from __dbt__CTE__intm_le_advances_ticket
union all
select * from __dbt__CTE__intm_le_advances_ticket_ppv
)