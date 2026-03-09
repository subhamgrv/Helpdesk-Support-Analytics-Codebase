
select
    calendar_date,
    day_of_week,
    month_num,
    year_num,
    is_weekend,
    is_holiday,
    holiday_name,
    business_day_flag
from {{ ref('stg_calendar') }}