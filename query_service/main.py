"""Read API over the MongoDB collections, plus the query UI."""

import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pymongo import DESCENDING, MongoClient

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "metricflow")

app = FastAPI(title="MetricFlow Query Service")

client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _time_filter(start: Optional[float], end: Optional[float]) -> Dict[str, Any]:
    window: Dict[str, float] = {}
    if start is not None:
        window["$gte"] = start
    if end is not None:
        window["$lte"] = end
    return {"timestamp": window} if window else {}


def _run(collection: str, criteria: Dict[str, Any], limit: int, skip: int) -> Dict[str, Any]:
    cursor = (
        db[collection]
        .find(criteria, {"_id": False})
        .sort("timestamp", DESCENDING)
        .skip(skip)
        .limit(limit)
    )
    return {
        "total": db[collection].count_documents(criteria),
        "returned_from": skip,
        "results": list(cursor),
    }


@app.get("/")
def ui():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/services")
def services() -> Dict[str, List[str]]:
    """Distinct service names, for populating the filter dropdowns."""
    names = set(db["logs"].distinct("service")) | set(db["metrics"].distinct("service"))
    return {"services": sorted(names)}


@app.get("/api/logs")
def query_logs(
    service: Optional[str] = None,
    level: Optional[str] = None,
    search: Optional[str] = None,
    start: Optional[float] = None,
    end: Optional[float] = None,
    limit: int = Query(100, ge=1, le=1000),
    skip: int = Query(0, ge=0),
):
    criteria: Dict[str, Any] = _time_filter(start, end)
    if service:
        criteria["service"] = service
    if level:
        criteria["level"] = level.upper()
    if search:
        # Escaped so user input is treated as a literal substring, not a pattern.
        import re

        criteria["message"] = {"$regex": re.escape(search), "$options": "i"}

    return _run("logs", criteria, limit, skip)


@app.get("/api/metrics")
def query_metrics(
    service: Optional[str] = None,
    start: Optional[float] = None,
    end: Optional[float] = None,
    limit: int = Query(100, ge=1, le=1000),
    skip: int = Query(0, ge=0),
):
    criteria: Dict[str, Any] = _time_filter(start, end)
    if service:
        criteria["service"] = service

    return _run("metrics", criteria, limit, skip)


@app.get("/api/stats")
def stats():
    """Counts and level breakdown, refreshed by the UI's live poll."""
    by_level = db["logs"].aggregate(
        [{"$group": {"_id": "$level", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
    )
    return {
        "logs": db["logs"].count_documents({}),
        "metrics": db["metrics"].count_documents({}),
        "levels": {row["_id"]: row["count"] for row in by_level},
    }


@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"mongo unavailable: {exc}")
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
