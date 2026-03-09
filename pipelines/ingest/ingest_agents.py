
import pandas as pd
from pathlib import Path
from pipelines.utils.db import get_engine


def run():
    path = Path("data/raw_files/agents.xlsx")
    df = pd.read_excel(path)
    df["source_file"] = path.name
    df.to_sql("agents", get_engine(), schema="raw", if_exists="append", index=False)
    print(f"Loaded {len(df)} agent rows")


if __name__ == "__main__":
    run()