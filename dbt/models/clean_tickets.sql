
with raw as (
    select
        ticket_id,
        category,
        sub_category,
        priority,
        created_date::timestamp as created_date,
        resolved_date::timestamp as resolved_date,
        status,
        assigned_group,
        technician,
        resolution_time_hrs,
        customer_impact
    from {{ source('itsm', 'itsm_raw_tickets') }}
),

deduped as (
    select *
    from raw
    qualify row_number() over (partition by ticket_id order by created_date desc) = 1
)

select *
from deduped
