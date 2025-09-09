with cleaned as (
    select * from {{ ref('clean_tickets') }}
)

select
    category,
    priority,
    round(avg(resolution_time_hrs)::numeric, 2) as avg_resolution_time_hrs,
    count(*) as ticket_count
from cleaned
group by category, priority
order by category, priority
