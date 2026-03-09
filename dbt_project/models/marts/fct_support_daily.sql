
with created_daily as (
    select
        created_date as support_date,
        count(distinct ticket_id) as tickets_created,
        avg(first_response_minutes) as avg_first_response_minutes,
        avg(resolution_minutes) as avg_resolution_minutes,
        sum(sla_breached_flag) as sla_breach_count,
        avg(customer_rating) as avg_customer_rating
    from {{ ref('fact_tickets') }}
    group by 1
),
resolved_daily as (
    select
        resolved_date as support_date,
        count(distinct ticket_id) as tickets_resolved
    from {{ ref('fact_tickets') }}
    where resolved_date is not null
    group by 1
),
backlog_daily as (
    select
        c.calendar_date as support_date,
        count(distinct f.ticket_id) as open_backlog
    from {{ ref('dim_calendar') }} c
    left join {{ ref('fact_tickets') }} f
      on f.created_date <= c.calendar_date
     and (f.resolved_date is null or f.resolved_date > c.calendar_date)
    group by 1
)

select
    c.calendar_date as support_date,
    coalesce(cd.tickets_created, 0) as tickets_created,
    coalesce(rd.tickets_resolved, 0) as tickets_resolved,
    coalesce(bd.open_backlog, 0) as open_backlog,
    cd.avg_first_response_minutes,
    cd.avg_resolution_minutes,
    coalesce(cd.sla_breach_count, 0) as sla_breach_count,
    cd.avg_customer_rating
from {{ ref('dim_calendar') }} c
left join created_daily cd
  on c.calendar_date = cd.support_date
left join resolved_daily rd
  on c.calendar_date = rd.support_date
left join backlog_daily bd
  on c.calendar_date = bd.support_date
order by 1