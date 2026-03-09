
select
    agent_id,
    agent_name,
    team_name,
    manager_name,
    location,
    active_flag,
    hire_date
from {{ ref('stg_agents') }}