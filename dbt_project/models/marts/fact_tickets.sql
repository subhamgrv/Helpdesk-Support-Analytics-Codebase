
with survey_per_ticket as (
    select
        ticket_id,
        max(customer_rating) as customer_rating,
        max(feedback_sentiment) as feedback_sentiment
    from {{ ref('stg_surveys') }}
    group by 1
)

select
    t.ticket_id,
    t.customer_id,
    t.department_id,
    t.agent_id,
    t.created_at,
    t.first_response_at,
    t.resolved_at,
    cast(t.created_at as date) as created_date,
    cast(t.resolved_at as date) as resolved_date,
    t.priority,
    t.status,
    t.channel,
    t.issue_type,
    t.first_response_minutes,
    t.resolution_minutes,
    t.sla_target_hours,
    t.sla_breached_flag,
    s.customer_rating,
    s.feedback_sentiment
from {{ ref('stg_tickets') }} t
left join survey_per_ticket s
    on t.ticket_id = s.ticket_id