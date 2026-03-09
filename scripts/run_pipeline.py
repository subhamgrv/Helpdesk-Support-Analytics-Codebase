
from pipelines.ingest.ingest_tickets import run as ingest_tickets
from pipelines.ingest.ingest_agents import run as ingest_agents
from pipelines.ingest.ingest_departments import run as ingest_departments
from pipelines.ingest.ingest_calendar import run as ingest_calendar
from pipelines.ingest.ingest_surveys import run as ingest_surveys
from pipelines.transform.staging_transform import run as staging_transform
from pipelines.quality.quality_checks import run as quality_checks


if __name__ == "__main__":
    ingest_tickets()
    ingest_agents()
    ingest_departments()
    ingest_calendar()
    ingest_surveys()
    staging_transform()
    quality_checks()
    print("SupportOps pipeline finished.")