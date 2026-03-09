
import random
from datetime import datetime, timedelta

from fastapi import FastAPI

app = FastAPI(title="SupportOps Satisfaction API")


@app.get("/surveys")
def get_surveys(limit: int = 1000):
    sentiments = ["positive", "neutral", "negative", "Positive", "NEGATIVE"]
    rows = []

    for i in range(1, limit + 1):
        rows.append({
            "survey_id": f"S{i:05d}",
            "ticket_id": f"T{i:05d}",
            "customer_rating": random.choice([1, 2, 3, 4, 5, 6]),
            "feedback_sentiment": random.choice(sentiments),
            "submitted_at": (datetime.now() - timedelta(days=random.randint(0, 90))).isoformat(),
        })

    return rows