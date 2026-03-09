
import pandas as pd
from sqlalchemy import text

from pipelines.utils.db import get_engine


def normalize_priority(value: str) -> str:
    if pd.isna(value):
        return "UNKNOWN"
    value = str(value).strip().lower()
    mapping = {
        "low": "LOW",
        "medium": "MEDIUM",
        "high": "HIGH",
        "urgent": "URGENT",
    }
    return mapping.get(value, value.upper())


def normalize_status(value: str) -> str:
    if pd.isna(value):
        return "UNKNOWN"
    value = str(value).strip().lower()
    mapping = {
        "open": "OPEN",
        "pending": "PENDING",
        "resolved": "RESOLVED",
        "closed": "CLOSED",
    }
    return mapping.get(value, value.upper())


def normalize_sentiment(value: str) -> str:
    if pd.isna(value):
        return "UNKNOWN"
    value = str(value).strip().lower()
    mapping = {
        "positive": "POSITIVE",
        "neutral": "NEUTRAL",
        "negative": "NEGATIVE",
    }
    return mapping.get(value, value.upper())


def normalize_team_name(value: str) -> str:
    if pd.isna(value):
        return "UNKNOWN"
    return str(value).strip().upper().replace(" ", "_")


def run():
    engine = get_engine()

    tickets = pd.read_sql("select * from raw.tickets", engine)
    agents = pd.read_sql("select * from raw.agents", engine)
    departments = pd.read_sql("select * from raw.departments", engine)
    calendar = pd.read_sql("select * from raw.calendar", engine)
    surveys = pd.read_sql("select * from raw.surveys", engine)

    tickets = tickets.drop_duplicates(subset=["ticket_id"], keep="last")
    tickets["created_at"] = pd.to_datetime(tickets["created_at"], errors="coerce")
    tickets["first_response_at"] = pd.to_datetime(tickets["first_response_at"], errors="coerce")
    tickets["resolved_at"] = pd.to_datetime(tickets["resolved_at"], errors="coerce")
    tickets["priority"] = tickets["priority"].apply(normalize_priority)
    tickets["status"] = tickets["status"].apply(normalize_status)

    departments = departments.drop_duplicates(subset=["department_id"], keep="last")
    departments["sla_hours"] = pd.to_numeric(departments["sla_hours"], errors="coerce").fillna(0).astype(int)

    agents = agents.drop_duplicates(subset=["agent_id"], keep="last")
    agents["team_name"] = agents["team_name"].apply(normalize_team_name)

    surveys = surveys.drop_duplicates(subset=["survey_id"], keep="last")
    surveys["submitted_at"] = pd.to_datetime(surveys["submitted_at"], errors="coerce")
    surveys["customer_rating"] = pd.to_numeric(surveys["customer_rating"], errors="coerce")
    surveys = surveys[surveys["customer_rating"].between(1, 5, inclusive="both")]
    surveys["customer_rating"] = surveys["customer_rating"].astype(int)
    surveys["feedback_sentiment"] = surveys["feedback_sentiment"].apply(normalize_sentiment)

    calendar = calendar.drop_duplicates(subset=["calendar_date"], keep="last")
    calendar["calendar_date"] = pd.to_datetime(calendar["calendar_date"], errors="coerce").dt.date
    calendar["day_of_week"] = pd.to_datetime(calendar["calendar_date"]).dt.day_name()
    calendar["month_num"] = pd.to_datetime(calendar["calendar_date"]).dt.month
    calendar["year_num"] = pd.to_datetime(calendar["calendar_date"]).dt.year
    calendar["business_day_flag"] = ~(calendar["is_weekend"].fillna(False) | calendar["is_holiday"].fillna(False))

    dept_lookup = departments[["department_id", "sla_hours"]].copy()
    tickets = tickets.merge(dept_lookup, on="department_id", how="left")
    tickets["sla_hours"] = tickets["sla_hours"].fillna(0)

    tickets["first_response_minutes"] = (
        (tickets["first_response_at"] - tickets["created_at"]).dt.total_seconds() / 60.0
    )
    tickets["resolution_minutes"] = (
        (tickets["resolved_at"] - tickets["created_at"]).dt.total_seconds() / 60.0
    )

    tickets.loc[tickets["first_response_minutes"] < 0, "first_response_minutes"] = None
    tickets.loc[tickets["resolution_minutes"] < 0, "resolution_minutes"] = None

    current_ts = pd.Timestamp.now()

    tickets["ticket_age_hours"] = (current_ts - tickets["created_at"]).dt.total_seconds() / 3600.0
    tickets["sla_target_hours"] = tickets["sla_hours"].astype(int)

    tickets["sla_breached_flag"] = (
        (
            (tickets["resolution_minutes"] / 60.0 > tickets["sla_target_hours"])
            | (
                tickets["resolved_at"].isna()
                & (tickets["ticket_age_hours"] > tickets["sla_target_hours"])
            )
        )
    ).astype(int)

    tickets = tickets.drop(columns=["ticket_age_hours", "sla_hours"])

    with engine.begin() as conn:
        conn.execute(text("truncate table staging.tickets_clean"))
        conn.execute(text("truncate table staging.agents_clean"))
        conn.execute(text("truncate table staging.departments_clean"))
        conn.execute(text("truncate table staging.calendar_clean"))
        conn.execute(text("truncate table staging.surveys_clean"))

    tickets.to_sql("tickets_clean", engine, schema="staging", if_exists="append", index=False)
    agents.to_sql("agents_clean", engine, schema="staging", if_exists="append", index=False)
    departments.to_sql("departments_clean", engine, schema="staging", if_exists="append", index=False)
    calendar.to_sql("calendar_clean", engine, schema="staging", if_exists="append", index=False)
    surveys.to_sql("surveys_clean", engine, schema="staging", if_exists="append", index=False)

    print("Staging transformations completed.")


if __name__ == "__main__":
    run()