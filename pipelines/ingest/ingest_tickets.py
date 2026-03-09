
import pandas as pd
from pathlib import Path
from pipelines.utils.db import get_engine


def run():
    path = Path("data/raw_files/tickets.csv")
    df = pd.read_csv(path)
    df["source_file"] = path.name
    df.to_sql("tickets", get_engine(), schema="raw", if_exists="append", index=False)
    print(f"Loaded {len(df)} ticket rows")
    

if __name__ == "__main__":
    run()