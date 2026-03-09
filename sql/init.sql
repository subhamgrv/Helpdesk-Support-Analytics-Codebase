create schema if not exists raw;
create schema if not exists staging;
create schema if not exists mart;
create schema if not exists meta;

create table if not exists raw.tickets (
    ticket_id text,
    customer_id text,
    department_id text,
    agent_id text,
    created_at timestamp,
    first_response_at timestamp,
    resolved_at timestamp,
    priority text,
    status text,
    channel text,
    issue_type text,
    ingestion_ts timestamp default now(),
    source_file text
);

create table if not exists raw.agents (
    agent_id text,
    agent_name text,
    team_name text,
    manager_name text,
    location text,
    active_flag boolean,
    hire_date date,
    ingestion_ts timestamp default now(),
    source_file text
);

create table if not exists raw.departments (
    department_id text,
    department_name text,
    sla_hours integer,
    ingestion_ts timestamp default now(),
    source_file text
);

create table if not exists raw.calendar (
    calendar_date date,
    is_weekend boolean,
    is_holiday boolean,
    holiday_name text,
    ingestion_ts timestamp default now(),
    source_file text
);

create table if not exists raw.surveys (
    survey_id text,
    ticket_id text,
    customer_rating integer,
    feedback_sentiment text,
    submitted_at timestamp,
    ingestion_ts timestamp default now(),
    source_file text
);

create table if not exists staging.tickets_clean as
select
    ticket_id,
    customer_id,
    department_id,
    agent_id,
    created_at,
    first_response_at,
    resolved_at,
    priority,
    status,
    channel,
    issue_type,
    cast(null as numeric(12,2)) as first_response_minutes,
    cast(null as numeric(12,2)) as resolution_minutes,
    cast(null as integer) as sla_target_hours,
    cast(null as integer) as sla_breached_flag
from raw.tickets where 1=0;

create table if not exists staging.agents_clean as
select * from raw.agents where 1=0;

create table if not exists staging.departments_clean as
select * from raw.departments where 1=0;

create table if not exists staging.calendar_clean as
select
    calendar_date,
    is_weekend,
    is_holiday,
    holiday_name,
    cast(null as text) as day_of_week,
    cast(null as integer) as month_num,
    cast(null as integer) as year_num,
    cast(null as boolean) as business_day_flag,
    ingestion_ts,
    source_file
from raw.calendar where 1=0;

create table if not exists staging.surveys_clean as
select * from raw.surveys where 1=0;

create table if not exists meta.pipeline_runs (
    run_id serial primary key,
    pipeline_name text,
    status text,
    started_at timestamp default now(),
    finished_at timestamp,
    message text
);
