
import os

import pandas as pd
import requests
from dotenv import load_dotenv

from pipelines.utils.db import get_engine

load_dotenv()


def run():
    url = os.getenv("SURVEY_API_URL", "http://localhost:8000/surveys")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    df = pd.DataFrame(response.json())
    df["source_file"] = "survey_api"
    df.to_sql("surveys", get_engine(), schema="raw", if_exists="append", index=False)
    print(f"Loaded {len(df)} survey rows")


if __name__ == "__main__":
    run()