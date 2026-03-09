
select
    department_id,
    department_name,
    sla_hours
from {{ ref('stg_departments') }}