
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)

BASE = Path("data/raw_files")
BASE.mkdir(parents=True, exist_ok=True)


def generate_departments():
    rows = [
        {"department_id": "D001", "department_name": "Technical Support", "sla_hours": 24},
        {"department_id": "D002", "department_name": "Billing", "sla_hours": 48},
        {"department_id": "D003", "department_name": "Account Management", "sla_hours": 72},
        {"department_id": "D004", "department_name": "General Inquiry", "sla_hours": 36},
    ]
    df = pd.DataFrame(rows)
    df.to_csv(BASE / "departments.csv", index=False)
    return df


def generate_agents(n=20):
    teams = ["Tier 1", "Tier 2", "Billing Ops", "Customer Care"]
    managers = ["Alice Brown", "David Smith", "Marta Klein", "Rahul Verma"]
    locations = ["Berlin", "Munich", "Hamburg", "Remote"]

    rows = []
    for i in range(1, n + 1):
        rows.append({
            "agent_id": f"A{i:03d}",
            "agent_name": fake.name(),
            "team_name": random.choice(teams),
            "manager_name": random.choice(managers),
            "location": random.choice(locations),
            "active_flag": random.choice([True, True, True, False]),
            "hire_date": fake.date_between(start_date="-4y", end_date="-30d"),
        })

    df = pd.DataFrame(rows)
    df.to_excel(BASE / "agents.xlsx", index=False)
    return df


def generate_calendar(days=120):
    start_date = datetime.now().date() - timedelta(days=days)
    holiday_map = {
        "2025-12-25": "Christmas",
        "2025-12-26": "Boxing Day",
        "2026-01-01": "New Year",
    }

    rows = []
    for i in range(days + 1):
        current = start_date + timedelta(days=i)
        current_str = current.isoformat()
        is_weekend = current.weekday() >= 5
        is_holiday = current_str in holiday_map
        rows.append({
            "calendar_date": current,
            "is_weekend": is_weekend,
            "is_holiday": is_holiday,
            "holiday_name": holiday_map.get(current_str),
        })

    df = pd.DataFrame(rows)
    df.to_csv(BASE / "calendar.csv", index=False)
    return df


def generate_tickets(agents, departments, n=2000):
    priorities = ["low", "medium", "high", "urgent", "HIGH", "Low"]
    statuses = ["open", "resolved", "pending", "closed", "Resolved"]
    channels = ["email", "chat", "phone", "web"]
    issue_types = ["login", "payment", "refund", "bug", "feature_request", "account_update"]

    rows = []
    now = datetime.now()

    active_agents = agents[agents["active_flag"] == True]
    if active_agents.empty:
        active_agents = agents.copy()

    for i in range(1, n + 1):
        created_at = now - timedelta(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

        department = departments.sample(1).iloc[0]
        agent = active_agents.sample(1).iloc[0]

        status = random.choice(statuses)

        if status.lower() in ["resolved", "closed"]:
            first_response_delay = timedelta(minutes=random.randint(5, 1440))
            resolution_delay = timedelta(hours=random.randint(1, 120))
            first_response_at = created_at + first_response_delay
            resolved_at = created_at + resolution_delay
        else:
            if random.random() < 0.75:
                first_response_at = created_at + timedelta(minutes=random.randint(5, 1440))
            else:
                first_response_at = None
            resolved_at = None

        rows.append({
            "ticket_id": f"T{i:05d}",
            "customer_id": f"C{random.randint(1, 700):04d}",
            "department_id": department["department_id"],
            "agent_id": agent["agent_id"],
            "created_at": created_at,
            "first_response_at": first_response_at,
            "resolved_at": resolved_at,
            "priority": random.choice(priorities),
            "status": status,
            "channel": random.choice(channels),
            "issue_type": random.choice(issue_types),
        })

    df = pd.DataFrame(rows)

    if len(df) >= 5:
        duplicate_rows = df.sample(5, random_state=42)
        df = pd.concat([df, duplicate_rows], ignore_index=True)

    df.to_csv(BASE / "tickets.csv", index=False)
    return df


if __name__ == "__main__":
    departments = generate_departments()
    agents = generate_agents()
    generate_calendar()
    generate_tickets(agents, departments)
    print("SupportOps synthetic data generated successfully.")