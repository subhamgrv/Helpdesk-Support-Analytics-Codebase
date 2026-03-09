
import pandas as pd
from pipelines.utils.db import get_engine


def run():
    engine = get_engine()
    checks = {}

    checks["duplicate_ticket_ids"] = pd.read_sql(
        """
        select count(*) as cnt
        from (
            select ticket_id
            from staging.tickets_clean
            group by 1
            having count(*) > 1
        ) t
        """,
        engine,
    ).iloc[0]["cnt"]

    checks["null_ticket_ids"] = pd.read_sql(
        "select count(*) as cnt from staging.tickets_clean where ticket_id is null",
        engine,
    ).iloc[0]["cnt"]

    checks["bad_first_response_times"] = pd.read_sql(
        """
        select count(*) as cnt
        from staging.tickets_clean
        where first_response_minutes < 0
        """,
        engine,
    ).iloc[0]["cnt"]

    checks["bad_resolution_times"] = pd.read_sql(
        """
        select count(*) as cnt
        from staging.tickets_clean
        where resolution_minutes < 0
        """,
        engine,
    ).iloc[0]["cnt"]

    checks["tickets_with_missing_agents"] = pd.read_sql(
        """
        select count(*) as cnt
        from staging.tickets_clean t
        left join staging.agents_clean a
          on t.agent_id = a.agent_id
        where a.agent_id is null
        """,
        engine,
    ).iloc[0]["cnt"]

    checks["tickets_with_missing_departments"] = pd.read_sql(
        """
        select count(*) as cnt
        from staging.tickets_clean t
        left join staging.departments_clean d
          on t.department_id = d.department_id
        where d.department_id is null
        """,
        engine,
    ).iloc[0]["cnt"]

    checks["invalid_survey_ratings"] = pd.read_sql(
        """
        select count(*) as cnt
        from staging.surveys_clean
        where customer_rating not between 1 and 5
        """,
        engine,
    ).iloc[0]["cnt"]

    checks["invalid_sla_hours"] = pd.read_sql(
        """
        select count(*) as cnt
        from staging.departments_clean
        where sla_hours <= 0
        """,
        engine,
    ).iloc[0]["cnt"]

    print("QUALITY CHECK REPORT")
    for name, value in checks.items():
        print(f"{name}: {value}")

    return checks


if __name__ == "__main__":
    run()