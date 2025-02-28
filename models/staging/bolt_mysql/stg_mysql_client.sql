{{ config(materialized="table") }}
with
    cte as (
        select *, row_number() over (partition by email order by created desc) rn
        from {{ source("bolt_mysql", "clients") }}
    )

select * except (rn)
from cte
where rn = 1 and email <> ''
