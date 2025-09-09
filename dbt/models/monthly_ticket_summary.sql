
with cleaned as (
    select * from {{ ref('clean_tickets') }}
),

monthly as (
    select
        date_trunc('month', created_date) as month,
        count(*) as total_tickets,
        round(avg(resolution_time_hrs)::numeric, 2) as avg_resolution_time,
        sum(case when status ilike 'Closed' then 1 else 0 end) as closed_tickets
    from cleaned
    group by 1
)

select
    month,
    total_tickets,
    avg_resolution_time,
    closed_tickets,
    round(closed_tickets::numeric / total_tickets * 100, 2) as closure_rate_pct
from monthly
order by month
