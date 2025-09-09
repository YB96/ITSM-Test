
with cleaned as (
    select * from {{ ref('clean_tickets') }}
),

group_stats as (
    select
        assigned_group,
        count(*) as total_tickets,
        sum(case when status ilike 'Closed' then 1 else 0 end) as closed_tickets
    from cleaned
    group by assigned_group
)

select
    assigned_group,
    total_tickets,
    closed_tickets,
    round(closed_tickets::numeric / total_tickets * 100, 2) as closure_rate_pct
from group_stats
order by closure_rate_pct desc
