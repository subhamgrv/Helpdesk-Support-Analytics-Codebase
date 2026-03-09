# Helpdesk Support Analytics Codebase

A end-to-end **data engineering & analytics pipeline** for helpdesk / customer-support operations — code-named **SupportOps**.  
The project ingests synthetic support data from flat files and a REST API, transforms and quality-checks it through staged layers, exposes analytics-ready data-mart tables via **dbt**, and orchestrates the whole flow with **Apache Airflow**.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Project Structure](#project-structure)
5. [Database Schema](#database-schema)
6. [Data Sources](#data-sources)
7. [Pipeline Stages](#pipeline-stages)
8. [dbt Models](#dbt-models)
9. [Airflow Orchestration](#airflow-orchestration)
10. [Quality Checks](#quality-checks)
11. [Getting Started](#getting-started)
12. [Configuration & Environment Variables](#configuration--environment-variables)
13. [Running the Pipeline Manually](#running-the-pipeline-manually)
14. [Key Metrics Produced](#key-metrics-produced)

---

## Project Overview

SupportOps is designed to answer questions such as:

- How many tickets were created / resolved each day?
- What is the running open-ticket backlog?
- Which agents or departments are breaching SLAs?
- What is the average customer satisfaction rating per day?
- How long does it take to respond to and resolve a ticket on average?

The platform generates realistic synthetic data, loads it through a **multi-layer pipeline** (raw → staging → mart), enforces data-quality rules, and materialises analytics tables ready for BI dashboards or ad-hoc SQL analysis.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         Apache Airflow DAG (daily)                           │
│                                                                              │
│  [generate_data]  ──►  [run_pipeline]  ──►  [dbt_run]  ──►  [dbt_test]     │
└──────────────────────────────────────────────────────────────────────────────┘
         │                      │
         ▼                      ▼
  data/raw_files/        PostgreSQL Database (supportops)
  ├── tickets.csv        ┌──────────────────────────────┐
  ├── agents.xlsx        │  Schema: raw                 │
  ├── departments.csv    │  ├── tickets                 │
  └── calendar.csv       │  ├── agents                  │
                         │  ├── departments              │
  Satisfaction API ─────►│  ├── calendar                │
  (FastAPI)              │  └── surveys                 │
                         │                              │
                         │  Schema: staging             │
                         │  ├── tickets_clean           │
                         │  ├── agents_clean            │
                         │  ├── departments_clean       │
                         │  ├── calendar_clean          │
                         │  └── surveys_clean           │
                         │                              │
                         │  Schema: mart (via dbt)      │
                         │  ├── fact_tickets            │
                         │  ├── fct_support_daily       │
                         │  ├── dim_agents              │
                         │  ├── dim_calendar            │
                         │  └── dim_departments         │
                         │                              │
                         │  Schema: meta                │
                         │  └── pipeline_runs           │
                         └──────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 16 |
| Containerisation | Docker / Docker Compose |
| Data generation | Python, Faker, pandas |
| Data ingestion | Python, pandas, SQLAlchemy, psycopg2 |
| Staging / transformation | Python, pandas, SQLAlchemy |
| REST API (survey data) | FastAPI + Uvicorn |
| Data modelling | dbt-postgres 1.8 |
| Orchestration | Apache Airflow 2.10 |
| Core libraries | pandas 2.2, SQLAlchemy 2.0, python-dotenv, requests, openpyxl |
| Language | Python 3.x |

---

## Project Structure

```
Helpdesk Support Analytics Codebase/
│
├── .env                          # Environment variables (DB credentials, API URL)
├── docker-compose.yml            # Spins up PostgreSQL container
├── requirements.txt              # Python dependencies
│
├── scripts/
│   ├── generate_data.py          # Synthetic data generator (tickets, agents, departments, calendar)
│   ├── run_pipeline.py           # Orchestrates all pipeline stages end-to-end
│   └── satisfaction_api.py       # FastAPI app exposing /surveys endpoint
│
├── pipelines/
│   ├── ingest/
│   │   ├── ingest_tickets.py     # Loads tickets.csv → raw.tickets
│   │   ├── ingest_agents.py      # Loads agents.xlsx → raw.agents
│   │   ├── ingest_departments.py # Loads departments.csv → raw.departments
│   │   ├── ingest_calendar.py    # Loads calendar.csv → raw.calendar
│   │   └── ingest_surveys.py     # Calls Satisfaction API → raw.surveys
│   │
│   ├── transform/
│   │   └── staging_transform.py  # Cleans & enriches raw → staging tables
│   │
│   ├── quality/
│   │   └── quality_checks.py     # SQL-based data quality report
│   │
│   └── utils/
│       └── db.py                 # SQLAlchemy engine factory (reads .env)
│
├── dbt_project/
│   ├── dbt_project.yml           # dbt project config (name: supportops_dbt)
│   ├── profiles.yml              # dbt connection profile (Postgres, schema: mart)
│   └── models/
│       ├── schema.yml            # Column-level tests (not_null, unique, relationships)
│       ├── staging/
│       │   ├── stg_tickets.sql
│       │   ├── stg_agents.sql
│       │   ├── stg_departments.sql
│       │   ├── stg_surveys.sql
│       │   └── stg_calendar.sql
│       └── marts/
│           ├── dim_agents.sql
│           ├── dim_calendar.sql
│           ├── dim_departments.sql
│           ├── fact_tickets.sql
│           └── fct_support_daily.sql
│
├── airflow/
│   └── dags/
│       └── supportops_dag.py     # Daily Airflow DAG (4 tasks)
│
├── sql/
│   └── init.sql                  # Creates all raw / staging / meta tables on DB start
│
└── data/
    ├── raw_files/                # Generated CSV/Excel source files land here
    ├── staging_files/            # (reserved for file-based staging output)
    └── curated/                  # (reserved for curated exports)
```

---

## Database Schema

The `supportops` PostgreSQL database contains four schemas:

### `raw` — Landing zone (append-only)

| Table | Key Columns | Description |
|---|---|---|
| `raw.tickets` | `ticket_id`, `customer_id`, `department_id`, `agent_id`, `created_at`, `first_response_at`, `resolved_at`, `priority`, `status`, `channel`, `issue_type` | One row per support ticket event |
| `raw.agents` | `agent_id`, `agent_name`, `team_name`, `manager_name`, `location`, `active_flag`, `hire_date` | Support agent master data |
| `raw.departments` | `department_id`, `department_name`, `sla_hours` | Department and SLA configuration |
| `raw.calendar` | `calendar_date`, `is_weekend`, `is_holiday`, `holiday_name` | Business calendar dimension |
| `raw.surveys` | `survey_id`, `ticket_id`, `customer_rating`, `feedback_sentiment`, `submitted_at` | Post-ticket satisfaction surveys |

All raw tables include `ingestion_ts` (load timestamp) and `source_file` (source identifier) audit columns.

### `staging` — Cleaned & enriched

| Table | Additional Columns vs Raw |
|---|---|
| `staging.tickets_clean` | `first_response_minutes`, `resolution_minutes`, `sla_target_hours`, `sla_breached_flag` |
| `staging.agents_clean` | Normalised `team_name` (UPPER_SNAKE_CASE) |
| `staging.departments_clean` | Validated `sla_hours` |
| `staging.calendar_clean` | `day_of_week`, `month_num`, `year_num`, `business_day_flag` |
| `staging.surveys_clean` | Normalised `feedback_sentiment`, validated `customer_rating` (1–5 only) |

### `mart` — Analytics-ready (managed by dbt)

See [dbt Models](#dbt-models) below.

### `meta`

| Table | Description |
|---|---|
| `meta.pipeline_runs` | Audit log of pipeline executions (name, status, start/end timestamps, message) |

---

## Data Sources

### Flat Files (generated by `scripts/generate_data.py`)

| File | Format | Records | Notes |
|---|---|---|---|
| `data/raw_files/tickets.csv` | CSV | ~2 005 rows | 2 000 tickets + 5 intentional duplicates; covers last 90 days |
| `data/raw_files/agents.xlsx` | Excel | 20 rows | Agents with team, manager, location, hire date |
| `data/raw_files/departments.csv` | CSV | 4 rows | Technical Support (24 h SLA), Billing (48 h), Account Management (72 h), General Inquiry (36 h) |
| `data/raw_files/calendar.csv` | CSV | 121 rows | Rolling 120-day calendar with weekend/holiday flags |

Raw data intentionally contains dirty values (mixed-case priorities/statuses, duplicate records) to exercise the transformation layer.

### Satisfaction API (`scripts/satisfaction_api.py`)

A lightweight **FastAPI** service running at `http://localhost:8000`:

| Endpoint | Method | Query Param | Returns |
|---|---|---|---|
| `/surveys` | GET | `limit` (default: 1000) | JSON array of survey records |

Survey records include:
- `survey_id` (S00001 – S{limit})
- `ticket_id` (maps to corresponding ticket)
- `customer_rating` (1–6; ratings of 6 are invalid and filtered in staging)
- `feedback_sentiment` (`positive`, `neutral`, `negative`, mixed-case intentional)
- `submitted_at`

---

## Pipeline Stages

### Stage 1 — Data Generation (`scripts/generate_data.py`)

Generates all synthetic source files using **Faker** with a fixed random seed (42) for reproducibility:

- **Departments**: 4 static entries with SLA definitions
- **Agents**: 20 agents across 4 teams (Tier 1, Tier 2, Billing Ops, Customer Care) in 4 locations
- **Calendar**: 121 days from ~4 months ago to today, with weekend and public-holiday flags
- **Tickets**: 2 000 tickets with randomised priorities, statuses, channels, issue types, and response/resolution timestamps; 5 duplicate rows inserted intentionally

### Stage 2 — Ingestion (`pipelines/ingest/`)

Each ingester loads one source into the `raw` schema via `pandas.DataFrame.to_sql()` in **append** mode:

| Module | Source | Destination |
|---|---|---|
| `ingest_tickets.py` | `data/raw_files/tickets.csv` | `raw.tickets` |
| `ingest_agents.py` | `data/raw_files/agents.xlsx` | `raw.agents` |
| `ingest_departments.py` | `data/raw_files/departments.csv` | `raw.departments` |
| `ingest_calendar.py` | `data/raw_files/calendar.csv` | `raw.calendar` |
| `ingest_surveys.py` | `GET $SURVEY_API_URL` | `raw.surveys` |

### Stage 3 — Staging Transform (`pipelines/transform/staging_transform.py`)

Reads from `raw.*`, applies the following transformations, then **truncates and reloads** all `staging.*` tables:

| Entity | Transformations Applied |
|---|---|
| **Tickets** | De-duplicate on `ticket_id`; parse datetime columns; normalise `priority` and `status` to uppercase enum values; compute `first_response_minutes`, `resolution_minutes`, `sla_target_hours`; set `sla_breached_flag` |
| **Agents** | De-duplicate on `agent_id`; normalise `team_name` to `UPPER_SNAKE_CASE` |
| **Departments** | De-duplicate on `department_id`; coerce `sla_hours` to int |
| **Surveys** | De-duplicate on `survey_id`; parse `submitted_at`; filter out ratings outside 1–5; normalise `feedback_sentiment` |
| **Calendar** | De-duplicate on `calendar_date`; add `day_of_week`, `month_num`, `year_num`, `business_day_flag` |

**SLA breach logic:**
- A ticket is considered breached if resolved and `resolution_minutes / 60 > sla_target_hours`, **or** if still open and `ticket_age_hours > sla_target_hours`.

**Normalisation functions:**

| Function | Input Examples | Output |
|---|---|---|
| `normalize_priority` | `"LOW"`, `"Low"`, `"low"` | `"LOW"` |
| `normalize_status` | `"Resolved"`, `"RESOLVED"` | `"RESOLVED"` |
| `normalize_sentiment` | `"Positive"`, `"NEGATIVE"` | `"POSITIVE"` / `"NEGATIVE"` |
| `normalize_team_name` | `"Billing Ops"` | `"BILLING_OPS"` |

### Stage 4 — Quality Checks (`pipelines/quality/quality_checks.py`)

Runs 8 SQL-based assertions against the `staging` schema and prints a **QUALITY CHECK REPORT**:

| Check | What It Validates |
|---|---|
| `duplicate_ticket_ids` | No duplicate ticket IDs in staging |
| `null_ticket_ids` | No NULL ticket IDs |
| `bad_first_response_times` | No negative first-response time in minutes |
| `bad_resolution_times` | No negative resolution time in minutes |
| `tickets_with_missing_agents` | All ticket agent IDs resolve to a known agent |
| `tickets_with_missing_departments` | All ticket department IDs resolve to a known department |
| `invalid_survey_ratings` | All survey ratings are between 1 and 5 |
| `invalid_sla_hours` | All departments have `sla_hours > 0` |

---

## dbt Models

The dbt project (`supportops_dbt`, version 1.0.0) targets the `mart` schema on the `supportops` Postgres database.

### Materialisation Strategy

| Layer | Materialisation |
|---|---|
| `models/staging/` | **View** |
| `models/marts/` | **Table** |

### Staging Models (views over `staging.*`)

| Model | Source Table |
|---|---|
| `stg_tickets` | `staging.tickets_clean` |
| `stg_agents` | `staging.agents_clean` |
| `stg_departments` | `staging.departments_clean` |
| `stg_surveys` | `staging.surveys_clean` |
| `stg_calendar` | `staging.calendar_clean` |

### Mart Models (materialised tables)

#### `dim_agents`
Agent dimension table: `agent_id`, `agent_name`, `team_name`, `manager_name`, `location`, `active_flag`, `hire_date`.

#### `dim_departments`
Department dimension: `department_id`, `department_name`, `sla_hours`.

#### `dim_calendar`
Calendar dimension: `calendar_date`, `day_of_week`, `month_num`, `year_num`, `is_weekend`, `is_holiday`, `holiday_name`, `business_day_flag`.

#### `fact_tickets`
Core fact table joining tickets with surveys:

| Column | Description |
|---|---|
| `ticket_id` | Unique ticket identifier |
| `customer_id` | Customer identifier |
| `department_id` | FK to `dim_departments` |
| `agent_id` | FK to `dim_agents` |
| `created_at` / `resolved_at` / `first_response_at` | Timestamps |
| `created_date` / `resolved_date` | Date-only versions |
| `priority` / `status` / `channel` / `issue_type` | Normalised categorical fields |
| `first_response_minutes` | Minutes from creation to first response |
| `resolution_minutes` | Minutes from creation to resolution |
| `sla_target_hours` | Department-defined SLA target |
| `sla_breached_flag` | 1 if SLA breached, 0 otherwise |
| `customer_rating` | Survey rating (1–5) |
| `feedback_sentiment` | Normalised sentiment (POSITIVE / NEUTRAL / NEGATIVE) |

#### `fct_support_daily`
Daily aggregate fact table joining calendar with ticket metrics:

| Column | Description |
|---|---|
| `support_date` | Calendar date |
| `tickets_created` | Number of tickets opened on this date |
| `tickets_resolved` | Number of tickets resolved on this date |
| `open_backlog` | Count of open tickets as of this date |
| `avg_first_response_minutes` | Average response time |
| `avg_resolution_minutes` | Average resolution time |
| `sla_breach_count` | Total SLA breaches on this date |
| `avg_customer_rating` | Average survey rating |

### dbt Tests (`schema.yml`)

| Model | Column | Tests |
|---|---|---|
| `stg_tickets` | `ticket_id` | `not_null`, `unique` |
| `stg_agents` | `agent_id` | `not_null`, `unique` |
| `stg_departments` | `department_id` | `not_null`, `unique` |
| `fact_tickets` | `ticket_id` | `not_null`, `unique` |
| `fact_tickets` | `agent_id` | `relationships` → `dim_agents.agent_id` |
| `fact_tickets` | `department_id` | `relationships` → `dim_departments.department_id` |
| `dim_calendar` | `calendar_date` | `not_null`, `unique` |

---

## Airflow Orchestration

The DAG `supportops_pipeline` runs **daily** (`@daily`, no catchup) and chains four tasks:

```
generate_data  ──►  run_pipeline  ──►  dbt_run  ──►  dbt_test
```

| Task ID | Command | What It Does |
|---|---|---|
| `generate_data` | `python scripts/generate_data.py` | Regenerates all synthetic source files |
| `run_pipeline` | `python scripts/run_pipeline.py` | Ingest → staging transform → quality checks |
| `dbt_run` | `cd dbt_project && dbt run --profiles-dir .` | Materialises all dbt marts |
| `dbt_test` | `cd dbt_project && dbt test --profiles-dir .` | Runs dbt schema tests |

---

## Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (for PostgreSQL)
- Python 3.10+ with `pip`
- (Optional) Apache Airflow environment for orchestration

### 1. Clone and Install Dependencies

```bash
git clone <repo-url>
cd "Helpdesk Support Analytics Codebase"
pip install -r requirements.txt
```

### 2. Start PostgreSQL

```bash
docker-compose up -d
```

This starts a PostgreSQL 16 container (`supportops_postgres`) on port **5432** and automatically runs `sql/init.sql` to create all schemas and tables.

### 3. Configure Environment

Copy or edit `.env` at the project root:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=supportops
POSTGRES_USER=supportops
POSTGRES_PASSWORD=supportops
SURVEY_API_URL=http://localhost:8000/surveys
```

### 4. Start the Satisfaction API

The survey ingest step requires the FastAPI service to be running:

```bash
uvicorn scripts.satisfaction_api:app --reload --port 8000
```

The API will be available at `http://localhost:8000/surveys`.

### 5. Generate Synthetic Data

```bash
python scripts/generate_data.py
```

Output files are written to `data/raw_files/`.

### 6. Run the Full Pipeline

```bash
python scripts/run_pipeline.py
```

This runs all ingestion modules, staging transforms, and quality checks in sequence.

### 7. Run dbt Models

```bash
cd dbt_project
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## Configuration & Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `supportops` | Database name |
| `POSTGRES_USER` | `supportops` | Database user |
| `POSTGRES_PASSWORD` | `supportops` | Database password |
| `SURVEY_API_URL` | `http://localhost:8000/surveys` | Satisfaction API endpoint |

All variables are loaded via `python-dotenv` from the `.env` file at the project root.

---

## Running the Pipeline Manually

Individual pipeline stages can be run independently:

```bash
# Ingest individual sources
python -m pipelines.ingest.ingest_tickets
python -m pipelines.ingest.ingest_agents
python -m pipelines.ingest.ingest_departments
python -m pipelines.ingest.ingest_calendar
python -m pipelines.ingest.ingest_surveys

# Run staging transforms
python -m pipelines.transform.staging_transform

# Run quality checks
python -m pipelines.quality.quality_checks
```

---

## Key Metrics Produced

The `mart.fct_support_daily` and `mart.fact_tickets` tables enable the following analytics:

- **Daily ticket volume** — tickets created and resolved per day
- **Open backlog trend** — running count of open tickets for any given date
- **Response time SLA** — average first-response and resolution times in minutes
- **SLA compliance** — count of breached tickets per day and per department
- **Customer satisfaction (CSAT)** — average rating and sentiment breakdown
- **Agent / team performance** — ticket volumes and response times by agent and team
- **Channel & issue-type distribution** — breakdown by email, chat, phone, web, and issue category
- **Business vs. weekend/holiday patterns** — filterable via `dim_calendar.business_day_flag`

---

## Notes

- The synthetic data generator (`generate_data.py`) uses `random.seed(42)` for reproducibility.
- Raw data **intentionally contains dirty values** (mixed-case statuses/priorities, duplicate rows, out-of-range ratings) to demonstrate the full cleaning pipeline.
- The staging transform uses a **truncate-and-reload** strategy; the raw layer is **append-only** to preserve full load history.
- The `meta.pipeline_runs` table is available as an audit log for custom pipeline run tracking.
- updated project setup notes on 2026-03-09
- updated project setup notes on 2026-03-09
